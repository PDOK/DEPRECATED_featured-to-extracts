FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured-to-extracts"
LABEL nl.pdok.version=$version

COPY artifacts/featured-to-extracts-web.jar /opt
WORKDIR /opt
CMD ["java", "-jar", "featured-to-extracts-web.jar"]
