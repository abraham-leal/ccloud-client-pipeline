FROM adoptopenjdk/openjdk8:alpine-slim
COPY target/lib /run/lib
COPY target/streams-pipeline-1.0.jar /run/streams-pipeline-1.0.jar
COPY streams-run.sh /run/streams-run.sh

WORKDIR /jolokia
# Installing Telegraf
ADD https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/1.6.2/jolokia-jvm-1.6.2-agent.jar /jolokia

ENV EXTRA_ARGS="-javaagent:/jolokia/jolokia-jvm-1.6.2-agent.jar=port=7773,host=*"

EXPOSE 7773

WORKDIR /run/
ENTRYPOINT ["sh", "streams-run.sh"]