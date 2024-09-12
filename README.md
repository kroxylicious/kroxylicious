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
    * [Kroxylicious Samples](#kroxylicious-samples)
    * [Kubernetes Examples](#kubernetes-examples)

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

## Kroxylicious Samples

See [kroxylicious-sample](kroxylicious-sample) to learn more about sample filters. Try them out and customise them for a
hands-on introduction to custom filters in Kroxylicious.

## Kubernetes Examples

[kubernetes-examples](kubernetes-examples/README.md) illustrate Kroxylicious running inside a Kubernetes
Cluster integrated with components such as Strimzi and cert-manager.
