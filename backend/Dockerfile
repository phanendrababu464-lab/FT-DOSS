# FT-DOSS Backend Dockerfile
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /ft-doss-gateway ./cmd/gateway/

FROM alpine:3.19
RUN apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=builder /ft-doss-gateway .

EXPOSE 8080 9090
CMD ["./ft-doss-gateway"]
