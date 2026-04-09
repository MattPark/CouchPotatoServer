FROM ghcr.io/linuxserver/baseimage-alpine:3.20

# set labels
LABEL maintainer="MattPark"
LABEL org.opencontainers.image.source="https://github.com/MattPark/CouchPotatoServer"
LABEL org.opencontainers.image.description="CouchPotato - Automatic movie downloading"

# environment
ENV COUCHPOTATO_DOCKER=1
ENV S6_KILL_GRACETIME=45000

# build args for baking version info (set by CI)
ARG VERSION_HASH=unknown
ARG VERSION_DATE=0
ARG VERSION_BRANCH=master

RUN \
  echo "**** install packages ****" && \
  apk add --no-cache \
    git \
    python3 \
    py3-pip \
    py3-lxml \
    mediainfo \
    libarchive-tools && \
  echo "**** install pip packages ****" && \
  pip3 install --no-cache-dir --break-system-packages \
    tinydb \
    requests \
    tornado \
    chardet \
    beautifulsoup4 \
    python-dateutil \
    "apscheduler>=3.10,<4" \
    html5lib \
    apprise \
    pyopenssl \
    cachelib \
    deluge-client \
    "bencode.py" \
    python-qbittorrent \
    rarfile \
    guessit && \
  echo "**** cleanup ****" && \
  rm -rf /tmp/*

# copy local files
COPY root/ /
COPY . /app/couchpotato

# bake version info into image
RUN echo "{\"hash\":\"${VERSION_HASH}\",\"date\":${VERSION_DATE},\"branch\":\"${VERSION_BRANCH}\"}" \
    > /app/couchpotato/version_info

# ports and volumes
EXPOSE 5050
VOLUME /config
