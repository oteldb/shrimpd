FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /shrimpd ./cmd/shrimpd

FROM alpine:3.21
WORKDIR /app
COPY --from=builder /shrimpd /shrimpd
RUN mkdir -p /data && chmod 750 /data
EXPOSE 8080
ENTRYPOINT ["/shrimpd"]
