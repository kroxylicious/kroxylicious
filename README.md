# kproxy

kproxy is an exploration for building a Kafka protocol proxy,
addressing use cases such as multi-tenancy, schema validation, or encryption.

## Build

Java, version 11 or newer, and Apache Maven are required for building this project.
Build the project like this:

```
$ mvn clean verify
```

The running of the tests can be controlled with the following Maven properties:

| property           | description            |
|--------------------|------------------------|
| `-DskipUTs=true`   | skip unit tests        |
| `-DskipITs=true`   | skip integration tests |
| `-DskipTests=true` | skip all tests         |

The kafka environment used by the integrations tests can be _defaulted_ with these two environment variables.

| env var                   | default | description                                                                                                  |
|---------------------------|---------|--------------------------------------------------------------------------------------------------------------|
| `TEST_CLUSTER_IN_VM`      | `true`  | if true, kafka will be run same virtual machines as the integration test. Otherwise containers will be used. |
| `TEST_CLUSTER_KRAFT_MODE` | `true`  | if true, kafka will be run in kraft mode.                                                                    |

When the integration-tests are run in container mode, the kafka/zookeeper logs are written to a location specified by
the `container.logs.dir`  system property. When run through Maven this is defaulted to `integrationtests/target/container-logs`.

Pass the `-Dquick` option to skip all tests and non-essential plug-ins and create the output artifact as quickly as possible:

```
$ mvn clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```
$ mvn process-sources
```

Build with the `dist` profile for creating an executable JAR:

```
$ mvn clean verify -Pdist -Dquick
```

Run the following to add missing license headers e.g. when adding new source files:

```
$ mvn org.commonjava.maven.plugins:directory-maven-plugin:highest-basedir@resolve-rootdir license:format
```

## Run

Build with the `dist` profile as shown above, then execute this:

```
$ java -jar kroxylicious/target/kproxy-1.0-SNAPSHOT.jar
```

## Performance Testing

See [benchmarking.md](benchmarking.md) for information on running basic performance tests for this proxy.

## Architecture Monitoring

This project uses [Deptective 🕵](https://github.com/moditect/deptective/) for monitoring its architecture and fails any violations,
either in form of unwanted package dependencies or circular package dependencies.
The target dependency model is configured in the [deptective.json](kroxylicious/src/main/resources/META-INF/deptective.json) file.
Any new package relationships need to be registered there.

To verify whether the code base adheres to that target model, run the following:

```
$ mvn clean verify -Dquick -Parchitecture-check -pl kroxylicious
```

In case of any architecture violations, the actual architecture can be visualized using GraphViz like so:

```
$ dot -Tpng kroxylicious/target/generated-sources/annotations/deptective.dot > kroxylicious-arch.png
```

## License

This code base is available under the Apache License, version 2.
