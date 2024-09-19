FROM maven:3.8.8
RUN mkdir /request-filter
ADD . /request-filter
WORKDIR /request-filter
RUN mvn clean install

FROM openjdk:21
COPY --from=0 /request-filter/target/smile_request_filter.jar /request-filter/smile_request_filter.jar
ENTRYPOINT ["java"]
