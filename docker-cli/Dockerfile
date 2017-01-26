FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured-to-extracts"
LABEL nl.pdok.version=$version

COPY artifacts/featured-to-extracts-cli.jar /opt
WORKDIR /opt
ENTRYPOINT ["java", "-jar", "featured-to-extracts-cli.jar"]
