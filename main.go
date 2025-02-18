package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
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
	clients     sync.Map
	redisClient *redis.Client
	upgrader    websocket.Upgrader
	db          *sql.DB
	incrScript  *redis.Script
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
	historyInterval   = 5 * time.Minute   // Log counter every 5 minutes
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
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_PASSWORD"),
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_PORT"),
		os.Getenv("POSTGRES_DB"),
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

	return &Server{
		redisClient: redisClient,
		upgrader:    upgrader,
		db:          db,
		incrScript:  incrScript,
	}
}

// Replace the Redis Incr calls with our Lua script
func (s *Server) incrementCounter(ctx context.Context, amount string) (string, error) {
	log.Printf("Executing increment script with amount: %s", amount)
	result, err := s.incrScript.Run(ctx, s.redisClient, []string{"counter"}, amount).Result()
	if err != nil {
		return "", fmt.Errorf("failed to execute increment script: %v", err)
	}
	log.Printf("Increment script result: %v", result)
	return result.(string), nil
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !websocket.IsWebSocketUpgrade(r) {
		w.WriteHeader(http.StatusOK)
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

	// Add close handler
	conn.SetCloseHandler(func(code int, text string) error {
		log.Printf("Client requested close with code %d: %s", code, text)
		if code == websocket.CloseNormalClosure || code == websocket.CloseGoingAway {
			// Send close message back to client for clean shutdown
			message := websocket.FormatCloseMessage(code, "")
			conn.WriteControl(websocket.CloseMessage, message, time.Now().Add(writeWait))
		}
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

		s.clients.Delete(clientID)
		log.Printf("Cleaned up client %s", clientID)
	}
	defer cleanup()

	s.clients.Store(clientID, conn)

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
				case "increment":
					// Handle user ID (can be UUID or OAuth ID)
					var userIDQuery string
					if msg.UserID != "" {
						// Try UUID first
						if _, err := uuid.Parse(msg.UserID); err == nil {
							userIDQuery = "id = $1"
						} else {
							// If not UUID, try OAuth ID
							userIDQuery = "oauth_id = $1"
						}
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
						log.Printf("New count after multiply by %d: %s", multiplyAmount, newCount)
					} else {
						// Normal increment
						valueDiff = new(big.Int).SetInt64(1)
						newCount, err = s.incrementCounter(ctx, "1")
						if err != nil {
							errorLog("Redis increment error: %v", err)
							continue
						}
						log.Printf("New count after increment: %s", newCount)
					}

					// Parse the string count to big.Int for the message
					parsedCount := new(big.Int)
					parsedCount.SetString(newCount, 10)
					log.Printf("Parsed count as big.Int: %s", parsedCount.String())

					// Update stats in background
					if msg.UserID != "" {
						go func(userID string, valueDiff *big.Int, countryCode, countryName string, operation string) {
							// First get the actual UUID from users table
							var dbUserID uuid.UUID
							err := s.db.QueryRowContext(context.Background(),
								fmt.Sprintf("SELECT id FROM users WHERE %s", userIDQuery),
								userID).Scan(&dbUserID)
							if err != nil {
								errorLog("Error finding user: %v", err)
								return
							}

							// Update user_stats table with the actual UUID
							_, err = s.db.ExecContext(context.Background(), `
								INSERT INTO user_stats (user_id, increment_count, last_increment)
								VALUES ($1, $2, NOW())
								ON CONFLICT (user_id)
								DO UPDATE SET
									increment_count = user_stats.increment_count + $2,
									last_increment = NOW()
							`, dbUserID, valueDiff.String())
							if err != nil {
								errorLog("Error updating user stats: %v", err)
							}

							// Update country stats if country info is provided
							if countryCode != "" && countryName != "" {
								_, err = s.db.ExecContext(context.Background(), `
									INSERT INTO country_stats (country_code, country_name, increment_count, last_increment)
									VALUES ($1, $2, $3, NOW())
									ON CONFLICT (country_code)
									DO UPDATE SET
										increment_count = country_stats.increment_count + $3,
										last_increment = NOW()
								`, countryCode, countryName, valueDiff.String())
								if err != nil {
									errorLog("Error updating country stats: %v", err)
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
					log.Printf("Publishing message with count: %s, operation: %s", data.Count, data.Operation)

					countBytes, err := json.Marshal(data)
					if err != nil {
						errorLog("Error marshaling count: %v", err)
						continue
					}

					if err := s.redisClient.Publish(ctx, "counter-channel", string(countBytes)).Err(); err != nil {
						errorLog("Error publishing count: %v", err)
					}
				case "ping":
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
		case <-done:
			return
		case err := <-errors:
			errorLog("Client error: %v", err)
			return
		case msg := <-messages:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			switch msg.Type {
			case "count":
				if err := conn.WriteJSON(msg); err != nil {
					errorLog("Error sending count: %v", err)
					return
				}
			case "increment":
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
			}
		}
	}
}

func (s *Server) startHistoryLogger(ctx context.Context) {
	ticker := time.NewTicker(historyInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				count, err := s.redisClient.Get(ctx, "counter").Result()
				if err != nil && err != redis.Nil {
					errorLog("Error getting counter for history: %v", err)
					continue
				}

				// Store as a sorted set with timestamp as score
				now := float64(time.Now().Unix())
				member := fmt.Sprintf("%d:%s", time.Now().Unix(), count)
				if err := s.redisClient.ZAdd(ctx, counterHistoryKey, &redis.Z{
					Score:  now,
					Member: member,
				}).Err(); err != nil {
					errorLog("Error storing counter history: %v", err)
				}

				// Cleanup old entries (keep last 30 days)
				cutoff := float64(time.Now().Add(-30 * 24 * time.Hour).Unix())
				if err := s.redisClient.ZRemRangeByScore(ctx, counterHistoryKey, "-inf", fmt.Sprintf("%f", cutoff)).Err(); err != nil {
					errorLog("Error cleaning up counter history: %v", err)
				}
			}
		}
	}()
}

func main() {
	server := NewServer()
	go server.startHistoryLogger(ctx)
	http.HandleFunc("/ws", server.handleWebSocket)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
