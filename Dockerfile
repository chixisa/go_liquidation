# 1. 编译阶段
FROM golang:1.24-alpine AS builder

# 代理配置 (保留你之前通的配置)
ENV HTTP_PROXY="http://192.168.1.5:10808"
ENV HTTPS_PROXY="http://192.168.1.5:10808"
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /app

# 【关键修改 1】安装编译 C 代码所需的工具 (gcc, musl-dev)
# 还要保留 git
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
    apk update && \
    apk add --no-cache git build-base

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# 【关键修改 2】开启 CGO，并添加 -tags musl 以适配 Alpine
# 必须设置 CGO_ENABLED=1 才能编译 confluent-kafka-go
RUN CGO_ENABLED=1 GOOS=linux go build -tags musl -o server main.go

# 2. 运行阶段
FROM alpine:latest

WORKDIR /app
# 换源 (可选，方便调试)
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

# 【可选】如果运行报错缺少库，取消下面这行的注释
# RUN apk add --no-cache libstdc++

COPY --from=builder /app/server .

CMD ["./server"]