FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/gradle:7.4.2-jdk11 AS build
WORKDIR /home/gradle/src
COPY --chown=gradle:gradle build.gradle settings.gradle ./
COPY --chown=gradle:gradle src ./src
RUN gradle shadowJar --no-daemon

FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/flink:1.19.1-scala_2.12-java11
USER root
COPY --from=build /home/gradle/src/build/libs/*-all.jar /opt/flink/usrlib/flink-kafka-demo.jar
COPY docker/submit-job.sh /opt/flink/submit-job.sh
RUN chmod +x /opt/flink/submit-job.sh
USER flink
ENTRYPOINT ["/opt/flink/submit-job.sh"]
