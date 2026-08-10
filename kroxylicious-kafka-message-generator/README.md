# kroxylicious-kafka-message-generator

A fork of the Apache Kafka `MessageGenerator` tool, used to generate `*Data` classes from the
Kafka protocol JSON IDL specs. These generated classes will form part of the stable public API
surface of Kroxylicious (see [proposal 116](https://github.com/kroxylicious/kroxylicious-design/blob/main/proposals/116-kafka-api-migration.md)).

## Origin

Forked from the [Apache Kafka](https://github.com/apache/kafka) repository at:

- **Tag:** `4.3.0`
- **Commit:** `a9ce3221537b8653448750697915607dc7936cf3`
- **Source path:** `generator/src/main/java/org/apache/kafka/message/`

The source files retain their original Apache Software Foundation copyright headers and are
redistributed under the Apache 2.0 licence, the same licence as the original.

## What changed from upstream

- Package renamed from `org.apache.kafka.message` to `io.kroxylicious.kafka.message`
- The `checker/` sub-package (schema evolution tooling with a JGit dependency) was not copied;
  it is not required for code generation

All other changes from this point forward are deliberate Kroxylicious modifications.

## Updating from upstream

When absorbing a new Kafka release:

1. Review the diff in `generator/src/main/java/org/apache/kafka/message/` between the old
   and new Kafka tags
2. Cherry-pick relevant changes into this module
3. Update the **Tag** and **Commit** recorded above