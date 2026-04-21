##############################################################################
# Stage 1: Build whisper-cli from whisper.cpp (CPU-only, static libs)
##############################################################################
FROM alpine:edge AS whisper-builder

RUN apk add --no-cache cmake g++ make git linux-headers

RUN git clone --depth 1 https://github.com/ggerganov/whisper.cpp.git /whisper

WORKDIR /whisper

RUN cmake -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_EXE_LINKER_FLAGS="-static-libgcc -static-libstdc++" \
      -DWHISPER_BUILD_TESTS=OFF \
      -DWHISPER_BUILD_EXAMPLES=OFF \
      -DWHISPER_BUILD_SERVER=OFF \
      -DBUILD_SHARED_LIBS=OFF \
    && cmake --build build --config Release -j$(nproc) --target whisper-cli

##############################################################################
# Stage 2: Runtime image
##############################################################################
FROM ghcr.io/linuxserver/baseimage-alpine:edge

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
    libarchive-tools \
    ffmpeg \
    libgomp && \
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
    guessit \
    pymediainfo && \
  echo "**** cleanup ****" && \
  rm -rf /tmp/*

# copy whisper-cli from builder stage
COPY --from=whisper-builder /whisper/build/bin/whisper-cli /usr/local/bin/whisper-cli

# copy local files
COPY root/ /
COPY . /app/couchpotato

# bake version info into image
RUN echo "{\"hash\":\"${VERSION_HASH}\",\"date\":${VERSION_DATE},\"branch\":\"${VERSION_BRANCH}\"}" \
    > /app/couchpotato/version_info

# ports and volumes
EXPOSE 5050
VOLUME /config
