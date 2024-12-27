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
	if env == "development" {
		envFile := fmt.Sprintf(".env.%s", env)
		err := godotenv.Load(envFile)
		if err != nil {
			log.Printf("Warning: Error loading %s: %v", envFile, err)
			if err := godotenv.Load(); err != nil {
				log.Printf("Warning: Error loading .env: %v", err)
			}
		} else {
			log.Printf("Loaded configuration from %s", envFile)
		}
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

	// Get initial count with retry
	var count int64
	var getErr error
	for i := 0; i < 3; i++ {
		log.Printf("Attempting to get initial count (attempt %d)", i+1)
		count, getErr = s.redisClient.Get(ctx, "counter").Int64()
		if getErr == redis.Nil {
			log.Printf("Counter not found, initializing to 0")
			count = 0
			if err := s.redisClient.Set(ctx, "counter", "0", 0).Err(); err != nil {
				log.Printf("Error initializing counter: %v", err)
			}
			break
		} else if getErr != nil {
			log.Printf("Redis get error (attempt %d): %v", i+1, getErr)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("Successfully got initial count: %d", count)
		break
	}

	if getErr != nil && getErr != redis.Nil {
		log.Printf("Failed to get initial count after retries: %v", getErr)
		return
	}

	// Send initial count
	initialMsg := Message{Type: "count", Count: count}
	log.Printf("Sending initial count to client %s: %d (raw message: %+v)", clientID, count, initialMsg)
	if err := conn.WriteJSON(initialMsg); err != nil {
		log.Printf("Error sending initial count to client %s: %v", clientID, err)
		return
	}
	log.Printf("Successfully sent initial count to client %s", clientID)

	// Subscribe to Redis updates
	pubsub := s.redisClient.Subscribe(ctx, "counter-channel")
	defer pubsub.Close()

	// Handle Redis messages
	go func() {
		defer pubsub.Close()
		log.Printf("Started Redis message handler for client %s", clientID)
		for {
			select {
			case <-done:
				log.Printf("Redis message handler stopping for client %s", clientID)
				return
			case msg := <-pubsub.Channel():
				var data struct {
					Count int64 `json:"count"`
				}
				log.Printf("Received Redis message: %s", msg.Payload)
				if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
					log.Printf("Error unmarshaling message for client %s: %v", clientID, err)
					errors <- fmt.Errorf("error unmarshaling message: %v", err)
					continue
				}
				log.Printf("Received Redis update for client %s: %d", clientID, data.Count)
				outMsg := Message{Type: "count", Count: data.Count}
				log.Printf("Sending message to client %s: %+v", clientID, outMsg)
				messages <- outMsg
			}
		}
	}()

	// Handle incoming WebSocket messages
	go func() {
		log.Printf("Started WebSocket message handler for client %s", clientID)
		for {
			select {
			case <-done:
				log.Printf("WebSocket message handler stopping for client %s", clientID)
				return
			default:
				var msg Message
				if err := conn.ReadJSON(&msg); err != nil {
					if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
						log.Printf("WebSocket read error for client %s: %v", clientID, err)
						errors <- fmt.Errorf("read error: %v", err)
					}
					return
				}

				if msg.Type == "increment" {
					log.Printf("Received increment request from client %s", clientID)
					messages <- msg
				}
			}
		}
	}()

	// Main event loop
	for {
		select {
		case <-done:
			log.Printf("Main event loop stopping for client %s", clientID)
			return
		case err := <-errors:
			log.Printf("Client %s error: %v", clientID, err)
			return
		case msg := <-messages:
			switch msg.Type {
			case "count":
				log.Printf("Sending count update to client %s: %+v", clientID, msg)
				if err := conn.WriteJSON(msg); err != nil {
					log.Printf("Error sending count to client %s: %v", clientID, err)
					return
				}
			case "increment":
				newCount, err := s.redisClient.Incr(ctx, "counter").Result()
				if err != nil {
					log.Printf("Redis increment error for client %s: %v", clientID, err)
					continue
				}
				log.Printf("Incremented counter for client %s to %d", clientID, newCount)

				data := struct {
					Count int64 `json:"count"`
				}{Count: newCount}

				countBytes, err := json.Marshal(data)
				if err != nil {
					log.Printf("Error marshaling count for client %s: %v", clientID, err)
					continue
				}

				log.Printf("Publishing count update: %s", string(countBytes))
				if err := s.redisClient.Publish(ctx, "counter-channel", string(countBytes)).Err(); err != nil {
					log.Printf("Error publishing count for client %s: %v", clientID, err)
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
