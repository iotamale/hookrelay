# Stage 1: Build
FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o hookrelay ./cmd/hookrelay

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/hookrelay .

CMD ["./hookrelay"]