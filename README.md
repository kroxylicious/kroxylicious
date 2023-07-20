# Kroxylicious

Kroxylicious is an exploration for building a Kafka protocol proxy,
addressing use cases such as multi-tenancy, schema validation, or encryption.

<!-- TOC -->
* [Kroxylicious](#kroxylicious)
  * [Quick Links](#quick-links)
  * [Build](#build)
    * [Formatting](#formatting)
  * [Run](#run)
    * [Run natively](#run-natively)
      * [Debugging](#debugging)
    * [Run on Minikube](#run-on-minikube)
  * [Samples](#samples)
  * [Rendering documentation](#rendering-documentation)
  * [Performance Testing](#performance-testing)
    * [Jenkins pipeline for performance](#jenkins-pipeline-for-performance)
  * [Architecture Monitoring](#architecture-monitoring)
  * [IntelliJ setup](#intellij-setup)
  * [License](#license)
  * [Setting Up in Windows Using WSL](#setting-up-in-windows-using-wsl)
    * [Installing WSL](#installing-wsl)
    * [Ensure appropriate tooling available](#ensure-appropriate-tooling-available)
  * [Releasing the project](#releasing-the-project)
<!-- TOC -->

## Quick Links
- [kroxylicious.io](https://www.kroxylicious.io)
- [Documentation](https://www.kroxylicious.io/kroxylicious)
- [GitHub design and discussion](https://github.com/kroxylicious/design)
- [Community Slack chat](https://kroxylicious.slack.com/)

## License

This code base is available under the Apache License, version 2.

## Developer Guide
See the [DEV_GUIDE.md](developer guide).

## Releasing this project
See [The release guide](docs/Releasing.adoc)

## Kroxylicious Samples

See [kroxylicious-sample](kroxylicious-sample) to learn more about sample filters. Try them out and customise them for a hands-on introduction to custom filters in Kroxylicious.

## Rendering documentation

The `docs` directory has some user documentation written in [AsciiDoc](https://docs.asciidoctor.org/asciidoc/latest/) format.
You can render it to HTML using:

```
mvn org.asciidoctor:asciidoctor-maven-plugin:process-asciidoc@convert-to-html
```

The output will be in `target/html/master.html`. 

## Performance Testing

See [benchmarking.md](benchmarking.md) for information on running basic performance tests for this proxy.

### Jenkins pipeline for performance
When a PR is created and the performance tests are needed, the following comment shall be added into the PR:

```
@strimzi-ci run perf
```

It will launch the `kroxylicious-performance-tests-pr` build, that will insert a comment with a summary into the PR comparing the results with the previous execution.

In case a manual execution needs to be done, this job is ready for that `kroxylicious-performance-tests`, where
some parameters can be configurable (NUMBER_OF_MESSAGES, MESSAGE_SIZE, PRODUCER_PROPERTIES)

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
