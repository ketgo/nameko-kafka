FROM python:3.6-buster

RUN apt-get update
RUN apt-get install -y pipenv

WORKDIR /test

COPY tests ./tests
RUN chmod +x ./tests/test_container/entrypoint.sh

ENTRYPOINT ["./tests/test_container/entrypoint.sh"]
