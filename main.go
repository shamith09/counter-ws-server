package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var ctx = context.Background()

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
	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
		Password: os.Getenv("REDIS_PASSWORD"),
		Username: os.Getenv("REDIS_USERNAME"),
		DB:       0,
	})

	// Test Redis connection
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Printf("Warning: Redis connection failed: %v", err)
	}

	// Configure WebSocket upgrader
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			// In production, you should check the origin
			return true
		},
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
	defer conn.Close()

	// Generate unique client ID
	clientID := uuid.New().String()
	s.clients.Store(clientID, conn)
	defer s.clients.Delete(clientID)

	// Send initial count
	count, err := s.redisClient.Get(ctx, "counter").Int64()
	if err == redis.Nil {
		count = 0
	} else if err != nil {
		log.Printf("Redis error: %v", err)
		return
	}
	conn.WriteJSON(Message{Type: "count", Count: count})

	// Subscribe to Redis counter updates
	pubsub := s.redisClient.Subscribe(ctx, "counter-channel")
	defer pubsub.Close()

	// Handle Redis messages in a goroutine
	go func() {
		for msg := range pubsub.Channel() {
			var count int64
			json.Unmarshal([]byte(msg.Payload), &count)
			conn.WriteJSON(Message{Type: "count", Count: count})
		}
	}()

	// Handle WebSocket messages
	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Printf("Read error: %v", err)
			break
		}

		if msg.Type == "increment" {
			// Increment counter in Redis
			newCount, err := s.redisClient.Incr(ctx, "counter").Result()
			if err != nil {
				log.Printf("Redis increment error: %v", err)
				continue
			}

			// Publish new count to all servers
			countBytes, _ := json.Marshal(newCount)
			s.redisClient.Publish(ctx, "counter-channel", string(countBytes))
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
