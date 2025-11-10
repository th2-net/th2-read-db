FROM amazoncorretto:11-alpine-jdk
WORKDIR /home
COPY ./app/build/docker .
ENTRYPOINT ["/home/service/bin/service"]