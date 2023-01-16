FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY ./app/build/docker .
ENTRYPOINT ["/home/service/bin/service"]