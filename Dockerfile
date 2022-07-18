FROM maven:3.6.1-jdk-11-slim
RUN mkdir /request-filter
ADD . /request-filter
WORKDIR /request-filter
RUN mvn clean install

FROM openjdk:11-slim
COPY --from=0 /request-filter/target/smile_request_filter.jar /request-filter/smile_request_filter.jar
ENTRYPOINT ["java"]
