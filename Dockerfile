FROM ghcr.io/linuxserver/baseimage-alpine:3.20

# set labels
LABEL maintainer="MattPark"
LABEL org.opencontainers.image.source="https://github.com/MattPark/CouchPotatoServer"
LABEL org.opencontainers.image.description="CouchPotato - Automatic movie downloading"

# environment
ENV COUCHPOTATO_DOCKER=1

RUN \
  echo "**** install packages ****" && \
  apk add --no-cache \
    git \
    python3 \
    py3-pip \
    py3-lxml && \
  echo "**** install pip packages ****" && \
  pip3 install --no-cache-dir --break-system-packages \
    tinydb && \
  echo "**** cleanup ****" && \
  rm -rf /tmp/*

# copy local files
COPY root/ /
COPY . /app/couchpotato

# ports and volumes
EXPOSE 5050
VOLUME /config
