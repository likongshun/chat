# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src

# 可通过 docker compose build --build-arg GOPROXY=... 指定可用的 Go 模块代理。
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

RUN apk add --no-cache ca-certificates git
COPY go.mod ./
RUN go mod download

COPY . .

# go.sum 必须覆盖源码和测试导入的完整依赖图。
# 仓库若尚未包含 go.sum，tidy 会在构建阶段生成；verify 校验模块缓存完整性。
RUN go mod tidy \
    && go mod verify \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/collector ./cmd/collector

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 10001 collector \
    && adduser -S -D -H -u 10001 -G collector collector \
    && mkdir -p /app/configs /data/wal \
    && chown -R collector:collector /app /data
WORKDIR /app

COPY --from=build --chown=collector:collector /out/collector /app/collector
COPY --from=build --chown=collector:collector /src/configs /app/configs

USER 10001:10001
ENV CONFIG_XLSX=/app/configs/modbus_device_config.xlsx
ENV WAL_DIR=/data/wal

ENTRYPOINT ["/app/collector"]
