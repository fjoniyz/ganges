FROM maven:3.9.4-amazoncorretto-20
COPY ./ /home
WORKDIR /home
RUN mvn clean install -DskipTests
ENTRYPOINT ["mvn", "exec:java", "-Dexec.mainClass=anoniks.Pipe"]
