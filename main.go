package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var ctx = context.Background()
var isProduction = false

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

	// Set production mode after loading env files
	isProduction = os.Getenv("GO_ENV") == "production"
}

type Server struct {
	clients     sync.Map
	redisClient *redis.Client
	upgrader    websocket.Upgrader
}

type Message struct {
	Type  string `json:"type"`
	Count int64  `json:"count"`
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
	}
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

	clientID := fmt.Sprintf("counter-%d", time.Now().UnixNano())
	messages := make(chan Message, 100)
	errors := make(chan error, 10)
	done := make(chan struct{})

	var wg sync.WaitGroup
	cleanup := func() {
		close(done)
		conn.Close()
		wg.Wait()
		close(messages)
		close(errors)
		s.clients.Delete(clientID)
	}
	defer cleanup()

	s.clients.Store(clientID, conn)

	// Get initial count
	count, err := s.redisClient.Get(ctx, "counter").Int64()
	if err == redis.Nil {
		count = 0
		if err := s.redisClient.Set(ctx, "counter", "0", 0).Err(); err != nil {
			errorLog("Error initializing counter: %v", err)
			return
		}
	} else if err != nil {
		errorLog("Redis error: %v", err)
		return
	}

	// Send initial count
	conn.SetWriteDeadline(time.Now().Add(writeWait))
	initialMsg := Message{Type: "count", Count: count}
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
				var data struct {
					Count int64 `json:"count"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
					errorLog("Error unmarshaling message: %v", err)
					errors <- fmt.Errorf("error unmarshaling message: %v", err)
					continue
				}
				select {
				case messages <- Message{Type: "count", Count: data.Count}:
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
					if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
						errorLog("WebSocket read error: %v", err)
						select {
						case errors <- fmt.Errorf("read error: %v", err):
						case <-done:
						}
					}
					return
				}

				switch msg.Type {
				case "increment":
					select {
					case messages <- msg:
					case <-done:
						return
					default:
						errorLog("Message channel full, dropping increment")
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

				data := struct {
					Count int64 `json:"count"`
				}{Count: newCount}

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
				count, err := s.redisClient.Get(ctx, "counter").Int64()
				if err != nil && err != redis.Nil {
					errorLog("Error getting counter for history: %v", err)
					continue
				}

				// Store as a sorted set with timestamp as score
				now := float64(time.Now().Unix())
				member := fmt.Sprintf("%d:%d", time.Now().Unix(), count)
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
	http.HandleFunc("/ws", server.handleWebSocket)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
