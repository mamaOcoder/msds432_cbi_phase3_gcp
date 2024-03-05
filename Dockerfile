# syntax=docker/dockerfile:1
FROM golang:latest
ENV PORT 8080
ENV HOSTDIR 0.0.0.0
ENV API_KEY AIzaSyB0kOn-0ZXamQlUvlSn6c2TKqCvAqPT5zQ

EXPOSE 8080
WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod tidy
COPY . ./
RUN mkdir -p logfiles
RUN go build -o /main
CMD [ "/main" ]