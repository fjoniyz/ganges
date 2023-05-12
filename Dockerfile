FROM ubuntu:latest
LABEL authors="root"
RUN apt-get update && apt-get install default-jdk -y && apt-get install maven -y
COPY ./ /home
WORKDIR /home
ENTRYPOINT ["mvn", "exec:java", "-Dexec.mainClass=myapps.Pipe"]