FROM java:8-jdk-alpine
COPY target/ccloud-client-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/app/
COPY client-run-class.sh /usr/app/
COPY secrets/producer.properties /usr/app/
WORKDIR /usr/app
EXPOSE 8081
ENTRYPOINT ["bash", "client-run-class.sh", "--producer-props producer.properties", "--topic myinternaltopic"]