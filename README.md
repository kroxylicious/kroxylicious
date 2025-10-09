# Kroxylicious

Kroxylicious, the snappy open source proxy for [Apache Kafka®](https://kafka.apache.org).

Kroxylicious is a Kafka protocol proxy, addressing use cases such as encryption, multi-tenancy and schema validation.

<!-- TOC -->
* [Kroxylicious](#kroxylicious)
  * [Quick Links](#quick-links)
  * [Build status](#build-status)
  * [License](#license)
  * [Developer Guide](#developer-guide)
  * [Releasing this project](#releasing-this-project)
  * [Performance Testing](#performance-testing)
  * [Contributing](#contributing)
<!-- TOC -->

## Quick Links
- [kroxylicious.io](https://www.kroxylicious.io)
- [Documentation](https://www.kroxylicious.io/kroxylicious)
- [GitHub design and discussion](https://github.com/kroxylicious/design)
- [Community Slack chat](https://kroxylicious.slack.com/)

## Build status
![Maven Central Version](https://img.shields.io/maven-central/v/io.kroxylicious/kroxylicious-parent)
 [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kroxylicious_kroxylicious&metric=coverage)](https://sonarcloud.io/summary/new_code?id=kroxylicious_kroxylicious)

## License

This code base is available under the [Apache License, version 2](LICENSE).

## Developer Guide

See the [Developer Guide](DEV_GUIDE.md).

## Releasing this project

See the [Release Guide](RELEASING.md)

## Performance Testing

See the [Performance Guide](PERFORMANCE.md) for information on running basic performance tests for this proxy.

## Kroxylicious Filter Development

Use [kroxylicious-filter-archetype](kroxylicious-filter-archetype) to get started developing a Custom Filter.

Run `mvn archetype:generate -DarchetypeGroupId=io.kroxylicious -DarchetypeArtifactId=kroxylicious-filter-archetype` to generate a standalone Filter project.

## Contributing


We welcome contributions! Please see our [contributing guidelines](https://github.com/kroxylicious/.github/blob/main/CONTRIBUTING.md) to get started.
