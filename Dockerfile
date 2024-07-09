FROM golang:1.22.5-alpine

COPY . /app
WORKDIR /app

RUN go build .
