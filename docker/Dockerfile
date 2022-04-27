FROM docker.io/997476274/csye7200-base:1.0

LABEL maintainer="Caspar" \
    email="chen.luo1@northeastern.edu"

USER root
RUN apt-get update -y
RUN apt-get install -y libgomp1
COPY . /app
WORKDIR ./app

ENTRYPOINT ["sbt", "run"]
