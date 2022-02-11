FROM golang:1.17 as monitor-build

COPY ./monitor /monitor
WORKDIR /monitor
RUN go build -o monitor .

FROM yandex/clickhouse-server:21.12
COPY --from=monitor-build /monitor ./monitor
RUN apt-get update && \
    apt-get -y install cron && \
    crontab /monitor/monitor-cron && \
    chmod +x /monitor/run.sh && \
    touch /var/log/cron.log

ENTRYPOINT ["/monitor/run.sh"]