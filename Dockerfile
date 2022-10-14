FROM ubuntu:18.04

# Postgres 13
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install curl gnupg lsb-release apt-utils -y
RUN curl -sSf https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
RUN cat /etc/apt/sources.list.d/pgdg.list
RUN apt-get update
RUN apt-get install gcc make libssl-dev autoconf pkg-config postgresql-13 postgresql-server-dev-13 libcurl4-gnutls-dev liblz4-dev libzstd-dev -y

COPY . /citus
RUN chown -R postgres:postgres /citus

# install into this postgres so we can run tests
USER postgres
WORKDIR /citus
ENV PATH=/usr/lib/postgresql/13/bin:$PATH
RUN ./configure && make clean extension

USER root
RUN make install-all

USER postgres
RUN make check-all

# tests passed, so export extension into /pg_ext
USER root
RUN DESTDIR=/pg_ext make -C /citus/src/backend/columnar install
