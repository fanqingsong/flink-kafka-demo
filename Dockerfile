FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY docker/maven-settings.xml /root/.m2/settings.xml
COPY pom.xml .
COPY src ./src
RUN mvn -B package -DskipTests

FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/flink:1.19.1-scala_2.12-java11
USER root
COPY --from=build /build/target/*-all.jar /opt/flink/usrlib/flink-kafka-demo.jar
COPY docker/submit-job.sh /opt/flink/submit-job.sh
RUN chmod +x /opt/flink/submit-job.sh
USER flink
ENTRYPOINT ["/opt/flink/submit-job.sh"]
