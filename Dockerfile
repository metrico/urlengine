FROM alpine as builder
RUN apk add --allow-untrusted --update --no-cache curl ca-certificates
WORKDIR /
RUN curl -fsSL github.com/metrico/urlengine/releases/latest/download/duckserver -O && chmod +x duckserver

FROM scratch
ENV PORT 80
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /duckserver /duckserver
EXPOSE 80
CMD ["/duckserver"]
