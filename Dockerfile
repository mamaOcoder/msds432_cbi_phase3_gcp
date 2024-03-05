# syntax=docker/dockerfile:1
FROM golang:latest
ENV PORT 8080
ENV HOSTDIR 0.0.0.0
ENV API_KEY AIzaSyBqA5yRUNeBgmjLGNSZHr4vJop_-lgYvlk

EXPOSE 8080
WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod tidy
COPY . ./
RUN mkdir -p logfiles
RUN go build -o /main
CMD [ "/main" ]