FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/notifyd . \
    && CGO_ENABLED=0 go build -trimpath -o /out/mockvendor ./cmd/mockvendor

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S notifyd \
    && adduser -S -G notifyd notifyd \
    && mkdir /data \
    && chown notifyd:notifyd /data
COPY --from=build /out/notifyd /usr/local/bin/notifyd
COPY --from=build /out/mockvendor /usr/local/bin/mockvendor

USER notifyd
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/notifyd"]
