FROM golang:1.11.2

FROM mysql:5.7

RUN apt-get update && apt-get install -y vim git curl procps ca-certificates gcc libc6-dev --no-install-recommends

WORKDIR /root

ENV MYSQL_ROOT_PASSWORD=root \
    GO111MODULE=on \
    PWD=/tmp \
    GOBIN=/root/mysql-replicator/compose \
    PATH="/usr/local/go/bin:${PATH}"

COPY compose/my.cnf /etc/mysql/my.cnf

COPY . /root/mysql-replicator/

COPY --from=0 /usr/local/go /usr/local


WORKDIR /root/mysql-replicator/compose/go

RUN /usr/local/bin/go install ./...

WORKDIR /root/mysql-replicator

ENTRYPOINT ["/root/mysql-replicator/compose/runner"]
