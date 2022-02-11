FROM golang:1.17
RUN mkdir /monitor
COPY ./monitor /monitor
WORKDIR /monitor
RUN go build -o monitor .

CMD ["./monitor"]