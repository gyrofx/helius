FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o helius .

FROM alpine:3.19
COPY --from=builder /app/helius /helius
COPY config.yaml /config.yaml
ENTRYPOINT ["/helius"]
