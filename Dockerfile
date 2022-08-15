FROM gradle:6.9.1-jdk11 AS build
ARG release_version
COPY ./ .
RUN gradle --no-daemon \
    :app:clean \
    :app:build \
    :app:dockerPrepare \
    -x :app:integrationTest \
    -Prelease_version=${release_version}

FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY --from=build /home/gradle/app/build/docker .
ENTRYPOINT ["/home/service/bin/service"]