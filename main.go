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
var isProduction = os.Getenv("GO_ENV") == "production"

// debugLog only logs in development
func debugLog(format string, v ...interface{}) {
	if !isProduction {
		log.Printf(format, v...)
	}
}

// errorLog always logs errors
func errorLog(format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}

func init() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "development"
	}

	if env == "development" {
		envFile := fmt.Sprintf(".env.%s", env)
		err := godotenv.Load(envFile)
		if err != nil {
			errorLog("Error loading %s: %v", envFile, err)
			if err := godotenv.Load(); err != nil {
				errorLog("Error loading .env: %v", err)
			}
		} else {
			debugLog("Loaded configuration from %s", envFile)
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
	Count int64  `json:"count"`
}

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period (must be less than pongWait)
	pingPeriod = (pongWait * 9) / 10
)

func NewServer() *Server {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort)

	opts := &redis.Options{
		Addr:     redisAddr,
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	}

	redisClient := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		errorLog("Redis connection failed: %v", err)
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

	clientID := uuid.New().String()
	debugLog("New client connected: %s", clientID)

	messages := make(chan Message)
	errors := make(chan error)
	done := make(chan struct{})

	defer func() {
		close(done)
		close(messages)
		close(errors)
		conn.Close()
		s.clients.Delete(clientID)
		debugLog("Client disconnected: %s", clientID)
	}()

	s.clients.Store(clientID, conn)

	// Get initial count
	count, err := s.redisClient.Get(ctx, "counter").Int64()
	if err == redis.Nil {
		count = 0
		if err := s.redisClient.Set(ctx, "counter", "0", 0).Err(); err != nil {
			errorLog("Error initializing counter: %v", err)
			return
		}
		debugLog("Initialized counter to 0")
	} else if err != nil {
		errorLog("Redis error: %v", err)
		return
	}
	debugLog("Got initial count: %d", count)

	// Send initial count with write deadline
	conn.SetWriteDeadline(time.Now().Add(writeWait))
	initialMsg := Message{Type: "count", Count: count}
	if err := conn.WriteJSON(initialMsg); err != nil {
		errorLog("Error sending initial count: %v", err)
		return
	}
	debugLog("Sent initial count: %d", count)

	pubsub := s.redisClient.Subscribe(ctx, "counter-channel")
	defer pubsub.Close()

	// Start ping ticker
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	// Handle Redis messages and pings
	go func() {
		defer pubsub.Close()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					errors <- fmt.Errorf("ping error: %v", err)
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
						errorLog("WebSocket read error: %v", err)
						errors <- fmt.Errorf("read error: %v", err)
					}
					return
				}

				switch msg.Type {
				case "increment":
					messages <- msg
				case "ping":
					// Respond to client ping with a pong message
					if err := conn.WriteJSON(Message{Type: "pong"}); err != nil {
						errorLog("Error sending pong: %v", err)
					}
				}
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case err := <-errors:
			errorLog("Client error: %v", err)
			return
		case msg := <-messages:
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

func main() {
	server := NewServer()
	http.HandleFunc("/ws", server.handleWebSocket)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}
