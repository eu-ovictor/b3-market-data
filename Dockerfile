FROM golang:1.22.5-alpine

COPY . /app
WORKDIR /app

RUN go build .

COPY wait_for_files.sh /wait_for_files.sh
RUN chmod +x /wait_for_files.sh
