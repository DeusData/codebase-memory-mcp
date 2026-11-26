FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS base
ENV TZ=UTC
FROM base
ENV APP_HOME=/app \
    # where the script lives
    APP_MODE=web
COPY run.sh /usr/local/bin/run.sh
ENTRYPOINT ["/usr/local/bin/run.sh"]
