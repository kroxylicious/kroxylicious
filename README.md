# kproxy

kproxy is an exploration for building a Kafka protocol proxy,
addressing use cases such as multi-tenancy, schema validation, or encryption.

## Build

Java, version 11 or newer, and Apache Maven are required for building this project.
Build the project like this:

```
$ mvn clean verify
```

Optionally, skip long-running system tests like this:

```
$ mvn clean verify -DexcludedGroups="system-test"
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

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

## Run

Build with the `dist` profile as shown above, then execute this:

```
$ java -jar target/kproxy-1.0-SNAPSHOT.jar
```

## License

This code base is available under the Apache License, version 2.
