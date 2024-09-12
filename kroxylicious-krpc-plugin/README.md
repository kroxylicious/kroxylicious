# krpc-code-gen

## What is it?

[Apache Kafka®][ak] describes its wire format using [message schemas][schemas] in a custom JSON format.
At compile time a [code generator][kafka_generator] generates Java source for classes to represent the protocol
messages, with methods to read/write to binary.

This project provides a similar code generation facility:

* The schema model is mostly the same.
* The generated code is defined in [Apache FreeMarker™][fm] templates, allowing you to generate _any_ kind of code (your
  choice of language) from the messages schemas.
* The code generation is packaged as a maven plugin for integration into your build process.

[ak]: https://kafka.apache.org

[fm]: https://freemarker.apache.org/

[schemas]: https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message

[kafka_generator]: https://github.com/apache/kafka/tree/trunk/generator/src/main/java/org/apache/kafka/message
