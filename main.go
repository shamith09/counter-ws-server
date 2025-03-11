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
	"os"
	"sync"
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
	clients        sync.Map
	redisClient    *redis.Client
	upgrader       websocket.Upgrader
	db             *sql.DB
	incrScript     *redis.Script
	rateLimiter    sync.Map  // clientID -> lastIncrementTime
	ipLimiter      sync.Map  // IP -> connectionInfo
	instanceID     string    // Unique ID for this server instance
	isLeader       bool      // Whether this instance is the leader
	leadershipChan chan bool // Channel to notify of leadership changes
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

	// Counter history keys
	counterHistoryKey = "counter_history" // Sorted set for counter history
	historyInterval   = 1 * time.Minute   // Log counter every minute for more detailed history
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
			return true
		},
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   1024,
		WriteBufferSize:  1024,
	}

	// Generate a unique ID for this server instance
	instanceID := fmt.Sprintf("server-%s", uuid.New().String())

	return &Server{
		redisClient:    redisClient,
		upgrader:       upgrader,
		db:             db,
		incrScript:     incrScript,
		instanceID:     instanceID,
		isLeader:       false,              // Start as non-leader
		leadershipChan: make(chan bool, 1), // Buffer of 1 to prevent blocking
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
	count := 0
	s.clients.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func (s *Server) updateViewerCount(ctx context.Context) {
	count := s.getViewerCount()
	if err := s.redisClient.Set(ctx, "viewer_count", count, 0).Err(); err != nil {
		errorLog("Error updating viewer count in Redis: %v", err)
	}
}

// isRateLimited checks if a client is rate limited for increment operations
// Returns true if the client is rate limited and should be blocked
func (s *Server) isRateLimited(clientID string) bool {
	// Rate limit: 5 increments per second per client
	rateLimit := 5 * time.Second

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
				// Update viewer count in Redis
				s.updateViewerCount(ctx)

				// Clean up old records from viewers table
				_, err := s.db.ExecContext(ctx,
					`DELETE FROM viewers WHERE last_seen < NOW() - INTERVAL '10 seconds'`)
				if err != nil {
					errorLog("Error cleaning up old viewer records: %v", err)
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

// runLeaderElection periodically attempts to become the leader
// Only one instance should be the leader and handle database writes
func (s *Server) runLeaderElection(ctx context.Context) {
	const leaderKey = "counter-leader"
	const leaderTTL = 30 * time.Second

	// Try to become leader immediately
	s.tryBecomeLeader(leaderKey, leaderTTL)

	// Then try periodically
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				// Release leadership if we're the leader
				isLeader := s.isLeader

				if isLeader {
					s.redisClient.Del(ctx, leaderKey)

					s.isLeader = false

					log.Printf("Instance %s released leadership", s.instanceID)

					// Notify about leadership change
					select {
					case s.leadershipChan <- false: // Signal we're no longer leader
					default: // Don't block if channel is full
					}
				}
				return
			case <-ticker.C:
				s.tryBecomeLeader(leaderKey, leaderTTL)
			}
		}
	}()
}

// tryBecomeLeader attempts to acquire leadership using Redis
func (s *Server) tryBecomeLeader(leaderKey string, leaderTTL time.Duration) {
	wasLeader := s.isLeader

	// If we're already the leader, just refresh the TTL
	if wasLeader {
		success, err := s.redisClient.Expire(ctx, leaderKey, leaderTTL).Result()
		if err != nil || !success {
			log.Printf("Failed to refresh leader key, releasing leadership: %v", err)
			s.isLeader = false

			// Notify about leadership change
			select {
			case s.leadershipChan <- false: // Signal we're no longer leader
			default: // Don't block if channel is full
			}
		}
		return
	}

	// Try to acquire leadership with NX (only if key doesn't exist)
	success, err := s.redisClient.SetNX(ctx, leaderKey, s.instanceID, leaderTTL).Result()
	if err != nil {
		log.Printf("Error in leader election: %v", err)
		return
	}

	if success {
		s.isLeader = true

		log.Printf("Instance %s became the leader", s.instanceID)

		// Notify about leadership change
		select {
		case s.leadershipChan <- true: // Signal we're now the leader
		default: // Don't block if channel is full
		}
	} else {
		// Check if the current leader is still alive
		currentLeader, err := s.redisClient.Get(ctx, leaderKey).Result()
		if err != nil && err != redis.Nil {
			log.Printf("Error checking current leader: %v", err)
			return
		}

		log.Printf("Current leader is %s, we are %s", currentLeader, s.instanceID)
	}
}

func (s *Server) handleViewerCount(w http.ResponseWriter, r *http.Request) {
	count := s.getViewerCount()

	// Update the count in Redis as well
	s.updateViewerCount(ctx)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]int{"count": count})
}

// handleHealthCheck provides information about this server instance
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	isLeader := s.isLeader

	response := map[string]interface{}{
		"status":      "ok",
		"instance_id": s.instanceID,
		"is_leader":   isLeader,
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

	clientID := fmt.Sprintf("counter-%d", time.Now().UnixNano())
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
			_, err := s.db.ExecContext(context.Background(), `DELETE FROM viewers WHERE client_id = $1`, clientID)
			if err != nil {
				errorLog("Error removing viewer from database: %v", err)
			}

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
		ipAddress := r.Header.Get("X-Forwarded-For")
		if ipAddress == "" {
			// Extract just the IP part from RemoteAddr (remove port if present)
			ipAddress = r.RemoteAddr
			if host, _, err := net.SplitHostPort(ipAddress); err == nil {
				ipAddress = host
			}
		}

		// Insert or update viewer record using direct query
		query := fmt.Sprintf(`
			INSERT INTO viewers (client_id, last_seen, ip_address, user_agent)
			VALUES ('%s', NOW(), '%s', '%s')
			ON CONFLICT (client_id) 
			DO UPDATE SET last_seen = NOW(), ip_address = '%s', user_agent = '%s'
		`, clientID, ipAddress, r.UserAgent(), ipAddress, r.UserAgent())

		_, err := s.db.ExecContext(context.Background(), query)

		if err != nil {
			errorLog("Error tracking viewer in database: %v", err)
		}

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
							// First get the actual UUID from users table
							var dbUserID uuid.UUID
							var userQuery string
							if _, err := uuid.Parse(userID); err == nil {
								userQuery = fmt.Sprintf("SELECT id FROM users WHERE id = '%s'", userID)
							} else {
								userQuery = fmt.Sprintf("SELECT id FROM users WHERE oauth_id = '%s'", userID)
							}

							err := s.db.QueryRowContext(context.Background(), userQuery).Scan(&dbUserID)
							if err != nil {
								errorLog("Error finding user: %v", err)
								return
							}

							// Update user_stats table with the actual UUID
							statsQuery := fmt.Sprintf(`
								INSERT INTO user_stats (user_id, increment_count, total_value_added, last_increment)
								VALUES ('%s', 1, '%s', NOW())
								ON CONFLICT (user_id)
								DO UPDATE SET
									increment_count = user_stats.increment_count + 1,
									total_value_added = user_stats.total_value_added + '%s',
									last_increment = NOW()
							`, dbUserID, valueDiff.String(), valueDiff.String())

							_, err = s.db.ExecContext(context.Background(), statsQuery)
							if err != nil {
								errorLog("Error updating user stats: %v", err)
							}

							// Insert user activity
							activityQuery := fmt.Sprintf(`
								INSERT INTO user_activity (user_id, value_diff, created_at)
								VALUES ('%s', '%s', NOW())
							`, dbUserID, valueDiff.String())

							_, err = s.db.ExecContext(context.Background(), activityQuery)
							if err != nil {
								errorLog("Error inserting user activity: %v", err)
							}

							// Update country stats if country info is provided
							if countryCode != "" && countryName != "" {
								countryStatsQuery := fmt.Sprintf(`
									INSERT INTO country_stats (country_code, country_name, increment_count, last_increment)
									VALUES ('%s', '%s', 1, NOW())
									ON CONFLICT (country_code)
									DO UPDATE SET
										increment_count = country_stats.increment_count + 1,
										last_increment = NOW()
								`, countryCode, countryName)

								_, err = s.db.ExecContext(context.Background(), countryStatsQuery)
								if err != nil {
									errorLog("Error updating country stats: %v", err)
								}

								// Insert country activity
								countryActivityQuery := fmt.Sprintf(`
									INSERT INTO country_activity (country_code, country_name, value_diff, created_at)
									VALUES ('%s', '%s', '%s', NOW())
								`, countryCode, countryName, valueDiff.String())

								_, err = s.db.ExecContext(context.Background(), countryActivityQuery)
								if err != nil {
									errorLog("Error inserting country activity: %v", err)
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
					// Handle ping message - update last_seen timestamp in a goroutine
					go func() {
						query := fmt.Sprintf(`
							UPDATE viewers 
							SET last_seen = NOW() 
							WHERE client_id = '%s'
						`, clientID)

						_, err := s.db.ExecContext(context.Background(), query)

						if err != nil {
							errorLog("Error updating viewer timestamp: %v", err)
						}
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
					query := fmt.Sprintf(`
						UPDATE viewers 
						SET last_seen = NOW() 
						WHERE client_id = '%s'
					`, clientID)

					_, err := s.db.ExecContext(context.Background(), query)

					if err != nil {
						errorLog("Error updating viewer timestamp: %v", err)
					}
				}()

				// No response needed for ping
			}
		}
	}
}

func (s *Server) startHistoryLogger(ctx context.Context) {
	ticker := time.NewTicker(historyInterval)

	// Function to write history to database
	writeHistory := func() {
		isLeader := s.isLeader

		// Only the leader should write history to the database
		if !isLeader {
			return
		}

		log.Printf("Leader %s is writing history to database", s.instanceID)
		count, err := s.redisClient.Get(ctx, "counter").Result()
		if err != nil && err != redis.Nil {
			errorLog("Error getting counter for history: %v", err)
			return
		}

		// Store in PostgreSQL counter_history table using a simple query
		_, err = s.db.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO counter_history (count, timestamp, granularity) VALUES ('%s', NOW(), 'detailed') ON CONFLICT (timestamp, granularity) DO UPDATE SET count = '%s'",
			count, count))

		if err != nil {
			errorLog("Error storing counter history in PostgreSQL: %v", err)
		}

		// Periodically aggregate detailed records into hourly and daily records
		// This is done less frequently to avoid excessive database operations
		hour := time.Now().Hour()
		minute := time.Now().Minute()

		// Run hourly aggregation at the start of each hour
		if minute < 5 {
			s.aggregateCounterHistory(ctx)
		}

		// Clean up old detailed records once a day at 1:00 AM
		if hour == 1 && minute < 5 {
			s.cleanupOldRecords(ctx)
		}
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				writeHistory()
			case isLeader := <-s.leadershipChan:
				// Leadership status changed
				if isLeader {
					log.Printf("Instance %s is now leader, will start writing history", s.instanceID)
					// Write history immediately when becoming leader
					writeHistory()
				} else {
					log.Printf("Instance %s is no longer leader, will stop writing history", s.instanceID)
				}
			}
		}
	}()
}

func (s *Server) aggregateCounterHistory(ctx context.Context) {
	// Aggregate detailed records into hourly records for the previous hour
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO counter_history (count, timestamp, granularity, start_count, end_count, avg_count, min_count, max_count)
		SELECT 
			(array_agg(count ORDER BY timestamp DESC))[1] as count,
			date_trunc('hour', timestamp) as hour_timestamp,
			'hourly' as granularity,
			(array_agg(count ORDER BY timestamp ASC))[1] as start_count,
			(array_agg(count ORDER BY timestamp DESC))[1] as end_count,
			(array_agg(count ORDER BY timestamp DESC))[1] as avg_count, -- Using last count as avg since we can't average strings
			min(count) as min_count,             -- This works for lexicographical comparison
			max(count) as max_count              -- This works for lexicographical comparison
		FROM counter_history
		WHERE 
			granularity = 'detailed' AND
			timestamp >= date_trunc('hour', NOW() - INTERVAL '1 hour') AND
			timestamp < date_trunc('hour', NOW())
		GROUP BY hour_timestamp
		ON CONFLICT (timestamp, granularity)
		DO UPDATE SET 
			count = EXCLUDED.count,
			start_count = EXCLUDED.start_count,
			end_count = EXCLUDED.end_count,
			avg_count = EXCLUDED.avg_count,
			min_count = EXCLUDED.min_count,
			max_count = EXCLUDED.max_count
	`)

	if err != nil {
		errorLog("Error aggregating hourly counter history: %v", err)
	}

	// At midnight, also aggregate the previous day's hourly records into a daily record
	if time.Now().Hour() == 0 {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO counter_history (count, timestamp, granularity, start_count, end_count, avg_count, min_count, max_count)
			SELECT 
				(array_agg(count ORDER BY timestamp DESC))[1] as count,
				date_trunc('day', timestamp) as day_timestamp,
				'daily' as granularity,
				(array_agg(count ORDER BY timestamp ASC))[1] as start_count,
				(array_agg(count ORDER BY timestamp DESC))[1] as end_count,
				(array_agg(count ORDER BY timestamp DESC))[1] as avg_count, -- Using last count as avg since we can't average strings
				min(count) as min_count,             -- This works for lexicographical comparison
				max(count) as max_count              -- This works for lexicographical comparison
			FROM counter_history
			WHERE 
				granularity = 'hourly' AND
				timestamp >= date_trunc('day', NOW() - INTERVAL '1 day') AND
				timestamp < date_trunc('day', NOW())
			GROUP BY day_timestamp
			ON CONFLICT (timestamp, granularity)
			DO UPDATE SET 
				count = EXCLUDED.count,
				start_count = EXCLUDED.start_count,
				end_count = EXCLUDED.end_count,
				avg_count = EXCLUDED.avg_count,
				min_count = EXCLUDED.min_count,
				max_count = EXCLUDED.max_count
		`)

		if err != nil {
			errorLog("Error aggregating daily counter history: %v", err)
		}
	}
}

func (s *Server) cleanupOldRecords(ctx context.Context) {
	// Keep detailed records for 7 days
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM counter_history 
		WHERE 
			granularity = 'detailed' AND 
			timestamp < NOW() - INTERVAL '7 days'
	`)

	if err != nil {
		errorLog("Error cleaning up old detailed records: %v", err)
	}

	// Keep hourly records for 90 days
	_, err = s.db.ExecContext(ctx, `
		DELETE FROM counter_history 
		WHERE 
			granularity = 'hourly' AND 
			timestamp < NOW() - INTERVAL '90 days'
	`)

	if err != nil {
		errorLog("Error cleaning up old hourly records: %v", err)
	}

	// Daily records are kept indefinitely for historical analysis
}

func main() {
	server := NewServer()

	log.Printf("Starting counter-ws-server with instance ID: %s", server.instanceID)

	// Start leader election before starting history logger
	go server.runLeaderElection(ctx)

	// These services run on all instances
	go server.startHistoryLogger(ctx) // Will only write if instance is leader
	go server.trackViewers(ctx)
	go server.cleanupRateLimiters(ctx)

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
