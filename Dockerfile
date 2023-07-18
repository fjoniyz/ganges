FROM maven:amazoncorretto
COPY ./ /home
WORKDIR /home
RUN mvn clean install -DskipTests
ENTRYPOINT ["mvn", "exec:java", "-Dexec.mainClass=myapps.Pipe"]
