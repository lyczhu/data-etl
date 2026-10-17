FROM azul/zulu-openjdk:17.0.17

WORKDIR /app

COPY target/registry-1.0.0.jar app.jar

EXPOSE 8761

ENTRYPOINT ["java", "-jar", "app.jar"]
