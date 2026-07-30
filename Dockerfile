FROM debian:bookworm-slim

WORKDIR /app

COPY xray /app/xray
COPY config.json /app/config.json

RUN chmod +x /app/xray

EXPOSE 8080

CMD ["/app/xray", "run", "-c", "/app/config.json"]