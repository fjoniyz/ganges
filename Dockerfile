FROM maven:amazoncorretto
COPY ./ /home
WORKDIR /home
RUN mvn clean install
ENTRYPOINT ["mvn", "exec:java", "-Dexec.mainClass=anoniks.Pipe"]