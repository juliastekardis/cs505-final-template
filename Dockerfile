FROM openjdk:8
RUN mkdir /myapp
COPY target/cs505-final-template-1.0-SNAPSHOT.jar /myapp
WORKDIR /myapp
CMD ["java", "-jar", "cs505-final-template-1.0-SNAPSHOT.jar"]
