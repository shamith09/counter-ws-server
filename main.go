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
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var ctx = context.Background()

func init() {
	// Load environment-specific .env file
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "development"
	}

	// Try to load environment-specific .env file first
	envFile := fmt.Sprintf(".env.%s", env)
	err := godotenv.Load(envFile)
	if err != nil {
		log.Printf("Warning: Error loading %s: %v", envFile, err)
		// Try to load default .env as fallback
		if err := godotenv.Load(); err != nil {
			log.Printf("Warning: Error loading .env: %v", err)
		}
	} else {
		log.Printf("Loaded configuration from %s", envFile)
	}
}

type Server struct {
	clients     sync.Map
	redisClient *redis.Client
	upgrader    websocket.Upgrader
}

type Message struct {
	Type  string `json:"type"`
	Count int64  `json:"count,omitempty"`
}

func NewServer() *Server {
	// Log environment variables (without password)
	log.Printf("Connecting to Redis at %s:%s with username: %s",
		os.Getenv("REDIS_HOST"),
		os.Getenv("REDIS_PORT"),
		os.Getenv("REDIS_USERNAME"))

	// Initialize Redis client with proper address formatting
	redisAddr := fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT"))
	opts := &redis.Options{
		Addr:     redisAddr,
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	}

	redisClient := redis.NewClient(opts)

	// Test Redis connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test Redis connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("Warning: Redis connection failed: %v", err)
		log.Printf("Redis connection details: %+v", opts)
	} else {
		log.Printf("Successfully connected to Redis at %s", redisAddr)
	}

	// Configure WebSocket upgrader
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
	// For health checks, return 200 if it's not a websocket upgrade request
	if !websocket.IsWebSocketUpgrade(r) {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Upgrade HTTP connection to WebSocket
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}

	// Generate unique client ID
	clientID := uuid.New().String()
	log.Printf("New client connected: %s", clientID)

	// Create message channels
	messages := make(chan Message)
	errors := make(chan error)
	done := make(chan struct{})

	// Ensure cleanup happens
	defer func() {
		close(done)
		close(messages)
		close(errors)
		conn.Close()
		s.clients.Delete(clientID)
		log.Printf("Client disconnected: %s", clientID)
	}()

	s.clients.Store(clientID, conn)

	// Get initial count
	count, err := s.redisClient.Get(ctx, "counter").Int64()
	if err == redis.Nil {
		count = 0
		if err := s.redisClient.Set(ctx, "counter", "0", 0).Err(); err != nil {
			log.Printf("Error initializing counter: %v", err)
		}
	} else if err != nil {
		log.Printf("Redis error: %v", err)
		return
	}

	// Send initial count
	if err := conn.WriteJSON(Message{Type: "count", Count: count}); err != nil {
		log.Printf("Error sending initial count: %v", err)
		return
	}

	// Subscribe to Redis updates
	pubsub := s.redisClient.Subscribe(ctx, "counter-channel")
	defer pubsub.Close()

	// Handle Redis messages
	go func() {
		defer pubsub.Close()
		for {
			select {
			case <-done:
				return
			case msg := <-pubsub.Channel():
				var data struct {
					Count int64 `json:"count"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
					errors <- fmt.Errorf("error unmarshaling message: %v", err)
					continue
				}
				messages <- Message{Type: "count", Count: data.Count}
			}
		}
	}()

	// Handle incoming WebSocket messages
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				var msg Message
				if err := conn.ReadJSON(&msg); err != nil {
					if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
						errors <- fmt.Errorf("read error: %v", err)
					}
					return
				}

				if msg.Type == "increment" {
					messages <- msg
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
			log.Printf("Client %s error: %v", clientID, err)
			return
		case msg := <-messages:
			switch msg.Type {
			case "count":
				if err := conn.WriteJSON(msg); err != nil {
					log.Printf("Error sending count to client %s: %v", clientID, err)
					return
				}
			case "increment":
				newCount, err := s.redisClient.Incr(ctx, "counter").Result()
				if err != nil {
					log.Printf("Redis increment error: %v", err)
					continue
				}

				data := struct {
					Count int64 `json:"count"`
				}{Count: newCount}

				countBytes, err := json.Marshal(data)
				if err != nil {
					log.Printf("Error marshaling count: %v", err)
					continue
				}

				if err := s.redisClient.Publish(ctx, "counter-channel", string(countBytes)).Err(); err != nil {
					log.Printf("Error publishing count: %v", err)
				}
			}
		}
	}
}

func main() {
	server := NewServer()

	// Handle WebSocket connections
	http.HandleFunc("/ws", server.handleWebSocket)

	// Start HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
