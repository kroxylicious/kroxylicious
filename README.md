# Kroxylicious

Kroxylicious is an exploration for building a Kafka protocol proxy,
addressing use cases such as multi-tenancy, schema validation, or encryption.

<!-- TOC -->
* [Kroxylicious](#kroxylicious)
  * [Quick Links](#quick-links)
  * [Build status](#build-status)
  * [License](#license)
  * [Developer Guide](#developer-guide)
  * [Releasing this project](#releasing-this-project)
  * [Performance Testing](#performance-testing)
  * [Kroxylicious Samples](#kroxylicious-samples)
  * [Architecture Monitoring](#architecture-monitoring)
<!-- TOC -->

## Quick Links
- [kroxylicious.io](https://www.kroxylicious.io)
- [Documentation](https://www.kroxylicious.io/kroxylicious)
- [GitHub design and discussion](https://github.com/kroxylicious/design)
- [Community Slack chat](https://kroxylicious.slack.com/)

## Build status
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

See [kroxylicious-sample](kroxylicious-sample) to learn more about sample filters. Try them out and customise them for a hands-on introduction to custom filters in Kroxylicious.

## Architecture Monitoring

This project uses [Deptective 🕵](https://github.com/moditect/deptective/) for monitoring its architecture and fails any violations,
either in form of unwanted package dependencies or circular package dependencies.
The target dependency model is configured in the [deptective.json](kroxylicious/src/main/resources/META-INF/deptective.json) file.
Any new package relationships need to be registered there.

To verify whether the code base adheres to that target model, run the following:

```bash
$ mvn clean verify -Dquick -Parchitecture-check -pl kroxylicious
```

In case of any architecture violations, the actual architecture can be visualized using GraphViz like so:

```bash
$ dot -Tpng kroxylicious/target/generated-sources/annotations/deptective.dot > kroxylicious-arch.png
```
