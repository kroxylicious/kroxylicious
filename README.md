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

## License

This code base is available under the Apache License, version 2.
