FROM golang:1.9 AS builder

WORKDIR /go/src/github.com/boivie/tsraw2json
COPY . /go/src/github.com/boivie/tsraw2json

RUN go get -d -v
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo .

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /go/src/github.com/boivie/tsraw2json/tsraw2json .

ENTRYPOINT ["/root/tsraw2json"]
