# Kroxylicious Sample

This sample filter project provides examples to help you learn how [custom filters](https://kroxylicious.io/documentation/@project.version@/html/developer-guide/) work in Kroxylicious. To learn more about Kroxylicious, visit the [docs](https://kroxylicious.io/documentation/@project.version@/). 

## Getting started

### Build

Building the sample project is easy!

```shell
mvn verify
```

### Run

```shell
chmod +x run.sh
./run.sh --config sample-proxy-config.yaml
```

### Configure

Filters can be added and removed by altering the `filters` list in the `sample-proxy-config.yaml` file. You can also reconfigure the sample filters by changing the configuration values in this file.

The **SampleFetchResponseFilter** and **SampleProduceRequestFilter** each have two configuration values that must be specified for them to work:

 - `findValue` - the string the filter will search for in the produce/fetch data
 - `replacementValue` - the string the filter will replace the value above with

#### Default Configuration


The default configuration for **SampleProduceRequestFilter** is:

```yaml
filtersDefinitions:
  - name: my-produce-replacer
    type: SampleProduceRequest
    config:
      findValue: foo
      replacementValue: bar
```

This means that it will search for the string `foo` in the produce data and replace all occurrences with the string `bar`. For example, if a Kafka Producer sent a produce request with data `{"myValue":"foo"}`, the filter would transform this into `{"myValue":"bar"}` and Kroxylicious would send that to the Kafka Broker instead. 

The default configuration for **SampleFetchResponseFilter** is:

```yaml
filterDefinitions:
  - name: my-fetch-replacer
    type: SampleFetchResponse
    config:
      findValue: bar
      replacementValue: baz
```

This means that it will search for the string `bar` in the fetch data and replace all occurrences with the string `baz`. For example, if a Kafka Broker sent a fetch response with data `{"myValue":"bar"}`, the filter would transform this into `{"myValue":"baz"}` and Kroxylicious would send that to the Kafka Consumer instead.