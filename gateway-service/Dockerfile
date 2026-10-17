FROM eclipse-temurin:17-jre-alpine

LABEL maintainer="lawyus"
LABEL description="API Gateway Service for Data ETL System"
LABEL version="1.0.0"

WORKDIR /app

RUN addgroup -S gateway && adduser -S gateway -G gateway

COPY target/lib/*.jar app.jar
COPY target/config/ config/

RUN chown -R gateway:gateway /app

USER gateway

EXPOSE 8080

ENV JAVA_OPTS="-Xms512m -Xmx1024m -XX:+UseG1GC"
ENV SPRING_PROFILES_ACTIVE="docker"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar --spring.config.location=classpath:/,file:./config/"]
