FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/coordinator ./cmd/coordinator
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/worker      ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/cli         ./cmd/cli

FROM alpine:3.21
RUN apk --no-cache add ca-certificates bash

COPY --from=builder /out/coordinator /coordinator
COPY --from=builder /out/worker      /worker
COPY --from=builder /out/cli         /cli

EXPOSE 50051 9090
