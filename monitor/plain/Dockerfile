FROM golang:1.17 as monitor-build

RUN mkdir /monitor
COPY ./monitor /monitor
WORKDIR /monitor
RUN go build -o monitor .

FROM yandex/clickhouse-server:21.12
COPY --from=monitor-build /monitor ./
ENTRYPOINT ["/bin/sh", "-c", "(./entrypoint.sh&) && ./monitor"]