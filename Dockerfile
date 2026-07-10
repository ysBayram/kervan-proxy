FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG BUILD_TAGS=""

RUN go build -tags "${BUILD_TAGS}" -o /app/kervan-proxy ./cmd/kervan-proxy

FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/kervan-proxy /usr/local/bin/kervan-proxy

EXPOSE 8080

ENTRYPOINT ["kervan-proxy"]
