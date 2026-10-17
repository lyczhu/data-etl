FROM azul/zulu-openjdk:17.0.17

WORKDIR /app

COPY target/config-server-1.0.0.jar app.jar

EXPOSE 8050

ENTRYPOINT ["java", "-jar", "app.jar"]
