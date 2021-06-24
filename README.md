# CMO Sample Validator & MetaDB Request Filter

The CMO sample validator and MetaDB request filter is a microservice for the MetaDB which subscribes to messages published by the LIMSRest service when a request is marked for delivery.

Messages received will undergo some validation steps to ensure that the request JSON delivered is complete. If a request is a CMO request (based on the JSON field `cmoRequest`) then it will undergo additional validation to ensure that all fields required for generating a CMO sample label are provided and contain valid values.

Invalid requests are logged and saved by the `RequestStatusLogger`.

Valid non-CMO requests are published to the IGO_NEW_REQUEST topic. Valid CMO requests are published to the CMO_LABEL_GENERATOR topic where they will be handled by the CMO Label Generator service which will then publish the request to IGO_NEW_REQUEST once CMO labels have been generated and added to the request JSON package.

## Run

### Custom properties

All properties are required with the exception of some NATS connection-specific properties. The following are only required if `nats.tls_channel` is set to `true`:

- `nats.keystore_path` : path to client keystore
- `nats.truststore_path` : path to client truststore
- `nats.key_password` : keystore password
- `nats.store_password` : truststore password

### Locally

**Requirements:**
- maven 3.6.1
- java 8

Add `application.properties` to the local application resources: `src/main/resources`

Build with

```
mvn clean install
```

Run with

```
java -jar target/cmo_metadb_request_filter.jar
```

### With Docker

**Requirements**
- docker

Build image with Docker

```
docker build -t <repo>/<tag>:<version> .
```

Push image to DockerHub

```
docker push <repo>/<tag>:<version>
```

If the Docker image is built with the properties baked in then simply run with:


```
docker run --name request-filter <repo>/<tag>:<version> \
	-jar /request-filter/cmo_metadb_request_filter.jar
```

Otherwise use a bind mount to make the local files available to the Docker image and add  `--spring.config.location` to the java arg

```
docker run --mount type=bind,source=<local path to properties files>,target=/request-filter/src/main/resources \
	--name request-filter <repo>/<tag>:<version> \
	-jar /request-filter/cmo_metadb_request_filter.jar \
	--spring.config.location=/request-filter/src/main/resources/application.properties
```
