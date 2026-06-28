# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build single static binary with no CGO requirements
RUN CGO_ENABLED=0 GOOS=linux go build -o causal-queue ./broker/broker.go

# Final scratch/light stage
FROM alpine:3.19

WORKDIR /app
RUN mkdir -p /app/data

# Copy binary and dashboard template
COPY --from=builder /app/causal-queue /app/causal-queue
COPY --from=builder /app/dashboard/index.html /app/dashboard/index.html
COPY --from=builder /app/config.yaml /app/config.yaml

EXPOSE 3000

ENTRYPOINT ["/app/causal-queue", "-port", "3000", "-db", "/app/data/queue.db"]
