# Counter WebSocket Server

WebSocket server for the global counter application. Handles real-time counter updates using Redis pub/sub.

## Development

```bash
# Install dependencies
go mod download

# Run server
go run main.go
```

## Environment Variables

```env
PORT=8080
REDIS_HOST=your-redis-host
REDIS_PORT=your-redis-port
REDIS_PASSWORD=your-redis-password
REDIS_USERNAME=your-redis-username
```

## Deployment

This server is designed to be deployed on Railway. To deploy:

1. Create a new project on Railway
2. Connect this repository
3. Add environment variables
4. Deploy!
