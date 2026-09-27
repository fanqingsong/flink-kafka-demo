FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/maven:3.9.9-eclipse-temurin-11 AS maven
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/eclipse-temurin:17-jdk AS build
COPY --from=maven /usr/share/maven /usr/share/maven
ENV PATH="/usr/share/maven/bin:${PATH}"
WORKDIR /build
COPY docker/maven-settings.xml /root/.m2/settings.xml
COPY pom.xml .
COPY flink-jobs/pom.xml flink-jobs/pom.xml
COPY sales-web/pom.xml sales-web/pom.xml
COPY sales-web/src sales-web/src
RUN mvn -B -pl sales-web -am package -DskipTests

FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/library/eclipse-temurin:17-jre
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/sales-web/target/sales-web-1.2.0.jar /app/app.jar
EXPOSE 8088
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
