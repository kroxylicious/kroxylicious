# Kroxylicious WASM Sample

This sample filter project provides examples to help you learn how [custom filters](https://kroxylicious.io/kroxylicious/#_custom_filters) work in Kroxylicious. To learn more about Kroxylicious, visit the [docs](https://kroxylicious.io/kroxylicious). 

## Getting started

### Build

Building the sample project is easy! You can build the **kroxylicious-sample** jar either on its own or with the rest of the Kroxylicious project.

#### To build all of Kroxylicious, including the sample:

```shell
mvn verify
```

#### To build the sample on its own:

```shell
mvn verify -pl :kroxylicious-wasm-sample-poc --also-make
```

> *__Note:__ If you build just the `kroxylicious-sample` module, you will need to also build the `kroxylicious-app` module separately (with `dist` profile, as shown below) in order to run the sample.*

#### Build with the `dist` profile for creating executable JARs:

```shell
mvn verify -Pdist -Dquick
```

> *__Note:__ You can leave out `--also-make` from these commands if you have already built the whole Kroxylicious project.*

### Run

Build both `kroxylicious-sample` and `kroxylicious-app` with the `dist` profile as above, then run the following command:

```shell
KROXYLICIOUS_CLASSPATH="kroxylicious-wasm-sample-poc/target/*" kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh --config kroxylicious-wasm-sample-poc/sample-proxy-config.yml
```

### Configure

Filters can be added and removed by altering the `filters` list in the `sample-proxy-config.yml` file. You can also reconfigure the sample filters by changing the configuration values in this file.

The **SampleFetchResponseFilter** and **SampleProduceRequestFilter** each have two configuration values that must be specified for them to work:

 - `replacerModule` - the string represent the wasm module to be loaded

#### Default Configuration


The default configuration for **SampleProduceRequestFilter** is:

```yaml
filters:
  - type: SampleProduceRequestFilterFactory
    config:
      replacerModule: foobar.wasm
```

This means that it will execute the `foobar.wasm` module compiled from the Rust program `foobar.wasm`. 

The default configuration for **SampleFetchResponseFilter** is:

```yaml
filters:
  - type: SampleFetchResponseFilterFactory
    config:
      replacerModule: barbaz.wasm
```

This means that it will execute the `barbaz.wasm` module compiled from the Rust program `barbaz.wasm`.
