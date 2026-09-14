FROM maven:3.9.9-eclipse-temurin-11 AS build
COPY job /build
WORKDIR /build
RUN mvn -q -DskipTests package

FROM apache/flink:1.20.1-scala_2.12-java11
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.20.1/flink-connector-base-1.20.1.jar /opt/flink/lib/
COPY --from=build /build/target/trend-topics-job-1.0.0.jar /opt/flink/usrlib/trend-topics-job.jar
