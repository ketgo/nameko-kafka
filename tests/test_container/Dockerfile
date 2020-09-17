FROM python:3-alpine

ARG PACKAGES='gcc musl-dev libressl-dev libffi-dev pcre pcre-dev python3-dev'

RUN apk update && apk add --no-cache ${PACKAGES}

WORKDIR /app

COPY setup.py .
RUN mkdir src
RUN pip install -e .

COPY src/ ./src
COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
