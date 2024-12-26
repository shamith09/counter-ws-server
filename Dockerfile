FROM golang:1.21-alpine

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o main .

# Expose the application port
EXPOSE 8080

# Run the application
CMD ["./main"] 