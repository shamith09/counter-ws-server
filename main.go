package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // PostgreSQL driver
)

var ctx = context.Background()

// errorLog always logs errors
func errorLog(format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}

func init() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "development"
	}

	// Try to load environment-specific file first
	envFile := fmt.Sprintf(".env.%s", env)
	err := godotenv.Load(envFile)
	if err != nil {
		// Fallback to .env file
		if err := godotenv.Load(); err != nil {
			log.Printf("Error loading .env: %v", err)
		}
	}
}

type Server struct {
	clients     sync.Map
	redisClient *redis.Client
	upgrader    websocket.Upgrader
	db          *sql.DB
	incrScript  *redis.Script
	rateLimiter sync.Map // clientID -> lastIncrementTime
	ipLimiter   sync.Map // IP -> connectionInfo
	instanceID  string   // Unique ID for this server instance
}

// ConnectionInfo tracks connection attempts from an IP
type ConnectionInfo struct {
	lastAttempt time.Time
	count       int
}

type Message struct {
	Type           string `json:"type"`
	Count          string `json:"count"`
	Operation      string `json:"operation"`
	MultiplyAmount int    `json:"multiply_amount,omitempty"`
	UserID         string `json:"user_id,omitempty"`
	Amount         string `json:"amount,omitempty"`
	CountryCode    string `json:"country_code,omitempty"`
	CountryName    string `json:"country_name,omitempty"`
}

type CounterMessage struct {
	Count          string `json:"count"`
	Operation      string `json:"operation"`
	MultiplyAmount int    `json:"multiply_amount,omitempty"`
}

type CounterState struct {
	Count          string `json:"count"`
	Operation      string `json:"operation"`
	MultiplyAmount int    `json:"multiply_amount,omitempty"`
}

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 20 * time.Second

	// Send pings to peer with this period (must be less than pongWait)
	pingPeriod = 10 * time.Second
)

func NewServer() *Server {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)

	if redisHost == "" || redisPort == "" {
		log.Fatalf("Redis host or port is empty - Host: '%s', Port: '%s'", redisHost, redisPort)
	}

	// Load the Lua script
	scriptBytes, err := os.ReadFile("bignum.lua")
	if err != nil {
		log.Fatalf("Failed to read Lua script: %v", err)
	}

	incrScript := redis.NewScript(string(scriptBytes))

	opts := &redis.Options{
		Addr:         redisAddr,
		Username:     os.Getenv("REDIS_USERNAME"),
		Password:     os.Getenv("REDIS_PASSWORD"),
		DB:           0,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	redisClient := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis at %s: %v", redisAddr, err)
	}

	// Load the script into Redis
	if err := incrScript.Load(ctx, redisClient).Err(); err != nil {
		log.Fatalf("Failed to load Lua script: %v", err)
	}

	// Connect to PostgreSQL
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_DB"),
		os.Getenv("POSTGRES_SSL_MODE"),
	)

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			// Get the origin header
			origin := r.Header.Get("Origin")
			if origin == "" {
				// Allow requests with no origin header (like curl requests)
				return true
			}

			// Parse the origin URL
			_, err := url.Parse(origin)
			if err != nil {
				log.Printf("Error parsing origin %s: %v", origin, err)
				return false
			}

			// Get allowed origins from environment variable
			allowedOriginsStr := os.Getenv("ALLOWED_ORIGINS")
			if allowedOriginsStr == "" {
				// Default to localhost and production domains if not specified
				allowedOriginsStr = "http://localhost:3000,https://thecounter.live"
			}

			// Split the allowed origins string into a slice
			allowedOrigins := strings.Split(allowedOriginsStr, ",")

			// Check if the origin is in the allowed list
			for _, allowed := range allowedOrigins {
				if strings.TrimSpace(allowed) == origin {
					return true
				}
			}

			log.Printf("Rejected WebSocket connection from origin: %s", origin)
			return false
		},
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   1024,
		WriteBufferSize:  1024,
	}

	return &Server{
		redisClient: redisClient,
		upgrader:    upgrader,
		db:          db,
		incrScript:  incrScript,
		instanceID:  uuid.New().String(), // Generate unique instance ID
	}
}

// Replace the Redis Incr calls with our Lua script
func (s *Server) incrementCounter(ctx context.Context, amount string) (string, error) {
	result, err := s.incrScript.Run(ctx, s.redisClient, []string{"counter"}, amount).Result()
	if err != nil {
		return "", fmt.Errorf("failed to execute increment script: %v", err)
	}

	return result.(string), nil
}

func (s *Server) getViewerCount() int {
	var count int

	// Prepare statement for viewer count
	stmt, err := s.db.PrepareContext(context.Background(),
		`SELECT COUNT(*) FROM viewers WHERE last_seen > NOW() - INTERVAL '30 seconds'`)
	if err != nil {
		errorLog("Error preparing viewer count statement: %v", err)
		inMemoryCount := 0
		s.clients.Range(func(_, _ interface{}) bool {
			inMemoryCount++
			return true
		})
		return inMemoryCount
	}
	defer stmt.Close()

	err = stmt.QueryRowContext(context.Background()).Scan(&count)

	if err != nil {
		errorLog("Error getting viewer count from database: %v", err)
		inMemoryCount := 0
		s.clients.Range(func(_, _ interface{}) bool {
			inMemoryCount++
			return true
		})
		return inMemoryCount
	}

	return count
}

func (s *Server) updateViewerCount(ctx context.Context) {
	count := s.getViewerCount()

	// Prepare statement for cleanup - only clean up stale viewers for this instance
	cleanupStmt, err := s.db.PrepareContext(ctx,
		`DELETE FROM viewers WHERE server_instance_id = $1 AND last_seen < NOW() - INTERVAL '30 seconds'`)
	if err != nil {
		errorLog("Error preparing cleanup statement: %v", err)
		return
	}
	defer cleanupStmt.Close()

	result, err := cleanupStmt.ExecContext(ctx, s.instanceID)
	if err != nil {
		errorLog("Error cleaning up stale viewers: %v", err)
	} else {
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			log.Printf("Cleaned up %d stale viewer records for instance %s", rowsAffected, s.instanceID)
		}
	}

	log.Printf("Current viewer count across all instances: %d", count)
}

// isRateLimited checks if a client is rate limited for increment operations
// Returns true if the client is rate limited and should be blocked
func (s *Server) isRateLimited(clientID string) bool {
	// Rate limit: 20 increments per second per client
	rateLimit := 50 * time.Millisecond

	// Check if client has a recent increment
	if lastTime, exists := s.rateLimiter.Load(clientID); exists {
		// If the last increment was less than rateLimit ago, block the request
		if time.Since(lastTime.(time.Time)) < rateLimit {
			return true
		}
	}

	// Update the last increment time
	s.rateLimiter.Store(clientID, time.Now())
	return false
}

// isIPRateLimited checks if an IP is making too many connection attempts
// Returns true if the IP should be blocked
func (s *Server) isIPRateLimited(ipAddress string) bool {
	// Rate limit: 10 connections per minute per IP
	const maxConnections = 10
	const resetPeriod = 1 * time.Minute

	now := time.Now()
	var info ConnectionInfo

	// Get current connection info for this IP
	if val, exists := s.ipLimiter.Load(ipAddress); exists {
		info = val.(ConnectionInfo)

		// Reset counter if it's been more than resetPeriod
		if now.Sub(info.lastAttempt) > resetPeriod {
			info.count = 0
		}
	}

	// Update connection info
	info.lastAttempt = now
	info.count++
	s.ipLimiter.Store(ipAddress, info)

	// Check if over limit
	return info.count > maxConnections
}

func (s *Server) trackViewers(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				// Update viewer count and clean up old records
				s.updateViewerCount(ctx)

				// Log current viewer count for this instance
				var instanceCount int
				instanceStmt, err := s.db.PrepareContext(ctx,
					`SELECT COUNT(*) FROM viewers WHERE server_instance_id = $1 AND last_seen > NOW() - INTERVAL '30 seconds'`)
				if err != nil {
					errorLog("Error preparing instance count statement: %v", err)
				} else {
					defer instanceStmt.Close()
					err = instanceStmt.QueryRowContext(ctx, s.instanceID).Scan(&instanceCount)
					if err != nil {
						errorLog("Error getting instance viewer count: %v", err)
					} else {
						log.Printf("Current active viewers for instance %s: %d", s.instanceID, instanceCount)
					}
				}

				// Log total active viewers
				var totalCount int
				if err := s.db.QueryRowContext(ctx,
					`SELECT COUNT(*) FROM viewers WHERE last_seen > NOW() - INTERVAL '30 seconds'`).Scan(&totalCount); err == nil {
					log.Printf("Total active viewers across all instances: %d", totalCount)
				}
			}
		}
	}()
}

// cleanupRateLimiters periodically removes old entries from rate limiters
func (s *Server) cleanupRateLimiters(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				now := time.Now()

				// Clean up client rate limiter
				s.rateLimiter.Range(func(key, value interface{}) bool {
					lastTime := value.(time.Time)
					// Remove entries older than 5 minutes
					if now.Sub(lastTime) > 5*time.Minute {
						s.rateLimiter.Delete(key)
					}
					return true
				})

				// Clean up IP rate limiter
				s.ipLimiter.Range(func(key, value interface{}) bool {
					info := value.(ConnectionInfo)
					// Remove entries older than 10 minutes
					if now.Sub(info.lastAttempt) > 10*time.Minute {
						s.ipLimiter.Delete(key)
					}
					return true
				})
			}
		}
	}()
}

func (s *Server) handleViewerCount(w http.ResponseWriter, r *http.Request) {
	count := s.getViewerCount()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int{"count": count})
}

// handleHealthCheck provides information about this server instance
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":      "ok",
		"instance_id": s.instanceID,
		"is_leader":   false,
		"timestamp":   time.Now().Format(time.RFC3339),
		"connections": s.getViewerCount(),
	}

	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !websocket.IsWebSocketUpgrade(r) {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Get client IP address
	ipAddress := r.Header.Get("X-Forwarded-For")
	if ipAddress == "" {
		// Extract just the IP part from RemoteAddr (remove port if present)
		ipAddress = r.RemoteAddr
		if host, _, err := net.SplitHostPort(ipAddress); err == nil {
			ipAddress = host
		}
	}

	// Check IP rate limiting
	if s.isIPRateLimited(ipAddress) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("Too many connection attempts. Please try again later."))
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		errorLog("Upgrade error: %v", err)
		return
	}

	// Configure WebSocket connection
	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Generate a unique client ID
	clientID := uuid.New().String()
	messages := make(chan Message, 100)
	errors := make(chan error, 10)
	done := make(chan struct{})

	var wg sync.WaitGroup
	cleanup := func() {
		select {
		case <-done:
			return
		default:
			close(done)
		}

		// Remove this client from our map
		s.clients.Delete(clientID)

		// Remove this viewer from the database immediately and update viewer count in a goroutine
		go func() {
			s.removeViewer(context.Background(), clientID)
			// Update viewer count after client disconnects
			s.updateViewerCount(context.Background())
		}()

		// Try to send close frame
		message := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Server shutting down")
		conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(writeWait))

		conn.Close()
		wg.Wait()

		// Clean up channels
		select {
		case <-messages:
		default:
			close(messages)
		}

		select {
		case <-errors:
		default:
			close(errors)
		}

		log.Printf("Cleaned up client %s", clientID)
	}
	defer cleanup()

	s.clients.Store(clientID, conn)

	// Track this viewer in the database in a goroutine
	go func() {
		s.trackViewer(context.Background(), clientID, ipAddress, r.UserAgent())
		// Update viewer count after a new client connects
		s.updateViewerCount(context.Background())
	}()

	// Get initial count
	count, err := s.redisClient.Get(ctx, "counter").Result()
	if err == redis.Nil {
		count = "0"
		if err := s.redisClient.Set(ctx, "counter", "0", 0).Err(); err != nil {
			errorLog("Error initializing counter: %v", err)
			return
		}
	} else if err != nil {
		errorLog("Redis error: %v", err)
		return
	}

	// Send initial count
	initialCount := new(big.Int)
	initialCount.SetString(count, 10)
	initialMsg := Message{Type: "count", Count: initialCount.String()}
	if err := conn.WriteJSON(initialMsg); err != nil {
		errorLog("Error sending initial count: %v", err)
		return
	}

	pubsub := s.redisClient.Subscribe(ctx, "counter-channel")
	defer pubsub.Close()

	// Start ping ticker
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	// Handle Redis messages and pings
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					select {
					case errors <- fmt.Errorf("ping error: %v", err):
					case <-done:
					}
					return
				}
			case msg := <-pubsub.Channel():
				if msg == nil {
					errors <- fmt.Errorf("received nil message from Redis pubsub")
					continue
				}
				var data struct {
					Count          string `json:"count"`
					Operation      string `json:"operation"`
					MultiplyAmount int    `json:"multiply_amount,omitempty"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
					errorLog("Error unmarshaling message: %v", err)
					errors <- fmt.Errorf("error unmarshaling message: %v", err)
					continue
				}
				if data.Count == "" {
					errorLog("Received empty count in message")
					errors <- fmt.Errorf("received empty count in message")
					continue
				}
				msgCount := new(big.Int)
				if _, ok := msgCount.SetString(data.Count, 10); !ok {
					errorLog("Invalid count format: %s", data.Count)
					errors <- fmt.Errorf("invalid count format: %s", data.Count)
					continue
				}
				select {
				case messages <- Message{
					Type:           "count",
					Count:          msgCount.String(),
					Operation:      data.Operation,
					MultiplyAmount: data.MultiplyAmount,
				}:
				case <-done:
					return
				default:
					errorLog("Message channel full, dropping message")
				}
			}
		}
	}()

	// Handle incoming WebSocket messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				var msg Message
				if err := conn.ReadJSON(&msg); err != nil {
					if websocket.IsUnexpectedCloseError(err,
						websocket.CloseNormalClosure,
						websocket.CloseGoingAway,
						websocket.CloseNoStatusReceived) {
						errorLog("WebSocket read error: %v", err)
						select {
						case errors <- fmt.Errorf("read error: %v", err):
						case <-done:
						}
					} else {
						log.Printf("Client %s closed connection normally", clientID)
					}
					return
				}

				switch msg.Type {
				case "close":
					log.Printf("Received close message from client %s", clientID)
					return
				case "get_viewer_count":
					// Send the current viewer count to the requesting client
					viewerCount := s.getViewerCount()
					if err := conn.WriteJSON(Message{
						Type:  "viewer_count",
						Count: fmt.Sprintf("%d", viewerCount),
					}); err != nil {
						errorLog("Error sending viewer count: %v", err)
					}
				case "increment":
					// Check rate limiting first
					if s.isRateLimited(clientID) {
						// Client is rate limited, send a rate limit message
						if err := conn.WriteJSON(Message{
							Type:  "rate_limited",
							Count: "Too many requests. Please wait a moment.",
						}); err != nil {
							errorLog("Error sending rate limit message: %v", err)
						}
						continue
					}

					// Get current count before any operation
					currentCount, err := s.redisClient.Get(ctx, "counter").Result()
					if err != nil {
						errorLog("Redis get error: %v", err)
						continue
					}
					currentBig := new(big.Int)
					currentBig.SetString(currentCount, 10)

					var newCount string
					var valueDiff *big.Int
					if msg.Operation == "multiply" {
						// Skip multiplication if amount is less than 1
						if msg.MultiplyAmount < 1 {
							continue
						}

						// If no multiply amount specified, default to 2
						multiplyAmount := 2
						if msg.MultiplyAmount > 0 {
							multiplyAmount = msg.MultiplyAmount
						}

						// Calculate new value
						multiplier := new(big.Int).SetInt64(int64(multiplyAmount))
						result := new(big.Int).Mul(currentBig, multiplier)

						// Calculate the difference
						valueDiff = new(big.Int).Sub(result, currentBig)

						// Log the multiplication details
						log.Printf("Multiplication: %s × %d = %s (adding %s to user stats)",
							currentBig.String(), multiplyAmount, result.String(), valueDiff.String())

						// Store the result
						newCount = result.String()
						if err := s.redisClient.Set(ctx, "counter", newCount, 0).Err(); err != nil {
							errorLog("Redis multiply error: %v", err)
							continue
						}
					} else {
						// Normal increment
						valueDiff = new(big.Int).SetInt64(1)
						newCount, err = s.incrementCounter(ctx, "1")
						if err != nil {
							errorLog("Redis increment error: %v", err)
							continue
						}
					}

					// Parse the string count to big.Int for the message
					parsedCount := new(big.Int)
					parsedCount.SetString(newCount, 10)

					// Update stats in background
					if msg.UserID != "" {
						go func(userID string, valueDiff *big.Int, countryCode, countryName string, operation string) {
							// Log the incoming user ID for debugging
							log.Printf("Processing increment attribution for userID: %s", userID)

							// First get the actual UUID from users table
							var dbUserID uuid.UUID
							var email string
							var username string

							// Prepare statement for user lookup
							stmt, err := s.db.PrepareContext(context.Background(), `
								SELECT id, email, username FROM users WHERE oauth_id = $1
							`)
							if err != nil {
								errorLog("Error preparing user lookup statement: %v", err)
								return
							}
							defer stmt.Close()

							// Try to look up user by oauth_id for Google users
							err = stmt.QueryRowContext(context.Background(), userID).Scan(&dbUserID, &email, &username)

							// If that fails, check if userID is a valid UUID and try to look it up directly
							if err != nil {
								if parsedUUID, err := uuid.Parse(userID); err == nil {
									// Prepare statement for direct UUID lookup
									directStmt, err := s.db.PrepareContext(context.Background(), `
										SELECT id, email, username FROM users WHERE id = $1
									`)
									if err != nil {
										errorLog("Error preparing direct UUID lookup statement: %v", err)
										return
									}
									defer directStmt.Close()

									err = directStmt.QueryRowContext(context.Background(), parsedUUID).Scan(&dbUserID, &email, &username)
								}
							}

							// If both lookups fail, try to find user by email as fallback
							if err != nil {
								// Check if userID looks like an email
								if strings.Contains(userID, "@") {
									// Prepare statement for email lookup
									emailStmt, err := s.db.PrepareContext(context.Background(), `
										SELECT id, email, username FROM users WHERE email = $1
									`)
									if err != nil {
										errorLog("Error preparing email lookup statement: %v", err)
										return
									}
									defer emailStmt.Close()

									err = emailStmt.QueryRowContext(context.Background(), userID).Scan(&dbUserID, &email, &username)
								}
							}

							if err != nil {
								errorLog("Failed to find user in database: %v, userID: %s", err, userID)
								return
							}

							log.Printf("Successfully found user with DB ID: %s for userID: %s", dbUserID.String(), userID)

							// Update user stats
							log.Printf("Updating user_stats for user %s with value_diff %s", dbUserID.String(), valueDiff.String())

							// Prepare statement for user stats
							userStatsStmt, err := s.db.PrepareContext(context.Background(), `
								INSERT INTO user_stats (user_id, increment_count, total_value_added, last_increment)
								VALUES ($1, 1, $2, NOW())
								ON CONFLICT (user_id)
								DO UPDATE SET
									increment_count = user_stats.increment_count + 1,
									total_value_added = user_stats.total_value_added + $2::numeric,
									last_increment = NOW()
							`)
							if err != nil {
								errorLog("Error preparing user stats statement: %v", err)
								return
							}
							defer userStatsStmt.Close()

							_, err = userStatsStmt.ExecContext(context.Background(), dbUserID, valueDiff.String())

							if err != nil {
								errorLog("Error updating user stats: %v", err)
							} else {
								log.Printf("Successfully updated user stats for user %s, added value: %s", dbUserID.String(), valueDiff.String())
							}

							// Insert user activity
							log.Printf("Inserting user_activity for user %s with value_diff %s", dbUserID.String(), valueDiff.String())

							// Prepare statement for user activity
							activityStmt, err := s.db.PrepareContext(context.Background(), `
								INSERT INTO user_activity (user_id, value_diff, created_at)
								VALUES ($1, $2, NOW())
							`)
							if err != nil {
								errorLog("Error preparing user activity statement: %v", err)
								return
							}
							defer activityStmt.Close()

							_, err = activityStmt.ExecContext(context.Background(), dbUserID, valueDiff.String())

							if err != nil {
								errorLog("Error inserting user activity: %v", err)
							}

							// Update country stats if country info is provided
							if countryCode != "" && countryName != "" {
								log.Printf("Updating country_stats for country %s (%s)", countryName, countryCode)

								// Prepare statement for country stats
								countryStmt, err := s.db.PrepareContext(context.Background(), `
									INSERT INTO country_stats (country_code, country_name, increment_count, last_increment)
									VALUES ($1, $2, 1, NOW())
									ON CONFLICT (country_code)
									DO UPDATE SET
										increment_count = country_stats.increment_count + 1,
										last_increment = NOW(),
										country_name = $2
								`)
								if err != nil {
									errorLog("Error preparing country stats statement: %v", err)
									return
								}
								defer countryStmt.Close()

								_, err = countryStmt.ExecContext(context.Background(), countryCode, countryName)

								if err != nil {
									errorLog("Error updating country stats: %v", err)
								}

								// Insert country activity
								log.Printf("Inserting country_activity for country %s (%s) with value_diff %s", countryName, countryCode, valueDiff.String())

								// Prepare statement for country activity
								countryActivityStmt, err := s.db.PrepareContext(context.Background(), `
									INSERT INTO country_activity (country_code, country_name, value_diff, created_at)
									VALUES ($1, $2, $3, NOW())
								`)
								if err != nil {
									errorLog("Error preparing country activity statement: %v", err)
									return
								}
								defer countryActivityStmt.Close()

								_, err = countryActivityStmt.ExecContext(context.Background(), countryCode, countryName, valueDiff.String())

								if err != nil {
									errorLog("Error inserting country activity: %v", err)
								} else {
									log.Printf("Successfully updated country stats for %s, added value: %s", countryName, valueDiff.String())
								}
							}
						}(msg.UserID, valueDiff, msg.CountryCode, msg.CountryName, msg.Operation)
					}

					data := struct {
						Count          string `json:"count"`
						Operation      string `json:"operation"`
						MultiplyAmount int    `json:"multiply_amount,omitempty"`
					}{
						Count:          parsedCount.String(),
						Operation:      msg.Operation,
						MultiplyAmount: msg.MultiplyAmount,
					}

					countBytes, err := json.Marshal(data)
					if err != nil {
						errorLog("Error marshaling count: %v", err)
						continue
					}

					if err := s.redisClient.Publish(ctx, "counter-channel", string(countBytes)).Err(); err != nil {
						errorLog("Error publishing count: %v", err)
					}
				case "ping":
					// Update the viewer's last_seen timestamp in a goroutine
					go func() {
						s.updateViewerTimestamp(context.Background(), clientID)
					}()

					// No response needed for ping
				case "pong":
					conn.SetWriteDeadline(time.Now().Add(writeWait))
					if err := conn.WriteJSON(Message{Type: "pong"}); err != nil {
						errorLog("Error sending pong: %v", err)
					}
				}
			}
		}
	}()

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case err := <-errors:
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			return
		case msg := <-messages:
			conn.SetWriteDeadline(time.Now().Add(writeWait))

			// Handle different message types
			switch msg.Type {
			case "count":
				if err := conn.WriteJSON(msg); err != nil {
					errorLog("Error sending count: %v", err)
					return
				}
			case "increment":
				// Handle increment operation
				newCount, err := s.redisClient.Incr(ctx, "counter").Result()
				if err != nil {
					errorLog("Redis increment error: %v", err)
					continue
				}

				newCountBig := new(big.Int)
				newCountBig.SetInt64(newCount)
				data := struct {
					Count          string `json:"count"`
					Operation      string `json:"operation"`
					MultiplyAmount int    `json:"multiply_amount,omitempty"`
				}{
					Count:          newCountBig.String(),
					Operation:      msg.Operation,
					MultiplyAmount: msg.MultiplyAmount,
				}

				countBytes, err := json.Marshal(data)
				if err != nil {
					errorLog("Error marshaling count: %v", err)
					continue
				}

				if err := s.redisClient.Publish(ctx, "counter-channel", string(countBytes)).Err(); err != nil {
					errorLog("Error publishing count: %v", err)
				}
			case "get_viewer_count":
				// Send current viewer count
				viewerCount := s.getViewerCount()
				response := Message{
					Type:  "viewer_count",
					Count: fmt.Sprintf("%d", viewerCount),
				}
				if err := conn.WriteJSON(response); err != nil {
					errorLog("Error sending viewer count: %v", err)
					return
				}

			case "ping":
				// Handle ping message - update last_seen timestamp in a goroutine
				go func() {
					s.updateViewerTimestamp(context.Background(), clientID)
				}()

				// No response needed for ping
			}
		}
	}
}

// Track viewer in database
func (s *Server) trackViewer(ctx context.Context, clientID, ipAddress, userAgent string) {
	// Prepare statement for tracking viewer
	stmt, err := s.db.PrepareContext(ctx, `
		INSERT INTO viewers (client_id, last_seen, ip_address, user_agent, server_instance_id)
		VALUES ($1, NOW(), $2, $3, $4)
		ON CONFLICT (client_id) 
		DO UPDATE SET last_seen = NOW(), ip_address = $2, user_agent = $3, server_instance_id = $4
	`)
	if err != nil {
		errorLog("Error preparing viewer tracking statement: %v", err)
		return
	}
	defer stmt.Close()

	// Execute the prepared statement
	_, err = stmt.ExecContext(ctx,
		clientID,
		ipAddress,
		userAgent,
		s.instanceID)

	if err != nil {
		errorLog("Error tracking viewer in database: %v", err)
	}
}

// Update viewer timestamp
func (s *Server) updateViewerTimestamp(ctx context.Context, clientID string) {
	// Prepare statement for updating timestamp
	stmt, err := s.db.PrepareContext(ctx, `
		UPDATE viewers 
		SET last_seen = NOW() 
		WHERE client_id = $1
	`)
	if err != nil {
		errorLog("Error preparing viewer timestamp update statement: %v", err)
		return
	}
	defer stmt.Close()

	// Execute the prepared statement
	_, err = stmt.ExecContext(ctx, clientID)

	if err != nil {
		errorLog("Error updating viewer timestamp: %v", err)
	}
}

// Remove viewer from database
func (s *Server) removeViewer(ctx context.Context, clientID string) {
	// Prepare statement for removing viewer
	stmt, err := s.db.PrepareContext(ctx, `
		DELETE FROM viewers 
		WHERE client_id = $1
	`)
	if err != nil {
		errorLog("Error preparing viewer removal statement: %v", err)
		return
	}
	defer stmt.Close()

	// Execute the prepared statement
	_, err = stmt.ExecContext(ctx, clientID)

	if err != nil {
		errorLog("Error removing viewer from database: %v", err)
	}
}

// Clean up stale viewers on startup
func (s *Server) cleanupStaleViewers() {
	// Prepare statement for cleanup
	stmt, err := s.db.PrepareContext(context.Background(),
		`DELETE FROM viewers WHERE last_seen < NOW() - INTERVAL '1 minute'`)
	if err != nil {
		errorLog("Error preparing startup cleanup statement: %v", err)
		return
	}
	defer stmt.Close()

	result, err := stmt.ExecContext(context.Background())

	if err != nil {
		errorLog("Error cleaning up stale viewers on startup: %v", err)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	log.Printf("Startup cleanup: Removed %d stale viewer records", rowsAffected)

	// Log the instance ID
	log.Printf("Server instance ID: %s", s.instanceID)
}

func main() {
	server := NewServer()

	// Clean up stale viewers on startup
	server.cleanupStaleViewers()

	// These services run on all instances
	go server.trackViewers(ctx)
	go server.cleanupRateLimiters(ctx)

	// Set up signal handling for graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		log.Println("Shutting down server...")

		// Clean up all viewers for this server instance
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Prepare statement for cleanup - only remove viewers for this instance
		stmt, err := server.db.PrepareContext(cleanupCtx, `DELETE FROM viewers WHERE server_instance_id = $1`)
		if err != nil {
			errorLog("Error preparing shutdown cleanup statement: %v", err)
		} else {
			defer stmt.Close()
			result, err := stmt.ExecContext(cleanupCtx, server.instanceID)
			if err != nil {
				errorLog("Error cleaning up viewers on shutdown: %v", err)
			} else if rowsAffected, err := result.RowsAffected(); err == nil {
				log.Printf("Shutdown cleanup: Removed %d viewer records for instance %s", rowsAffected, server.instanceID)
			}
		}

		os.Exit(0)
	}()

	http.HandleFunc("/ws", server.handleWebSocket)
	http.HandleFunc("/viewer-count", server.handleViewerCount)
	http.HandleFunc("/health-check", server.handleHealthCheck)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
