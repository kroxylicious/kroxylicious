# Micro Benchmarks

This module contains micro-benchmark code used for adhoc performance experiments.

## Running Benchmarks

### 1. Build the Project

From the [project root dir](..), run:

```shell
mvn clean install -DskipTests
```

### 2. Run Benchmarks

To run a benchmark
like [io.kroxylicious.microbenchmarks.InvokerScalabilityBenchmark](src/main/java/io/kroxylicious/microbenchmarks/InvokerScalabilityBenchmark.java)

```shell
java -jar kroxylicious-microbenchmarks/target/microbenchmarks.jar io.kroxylicious.microbenchmarks.InvokerScalabilityBenchmark
```

To run specific benchmark methods within a class you can use regex matching:

```shell
java -jar kroxylicious-microbenchmarks/target/microbenchmarks.jar io.kroxylicious.microbenchmarks.InvokerScalabilityBenchmark.methodNamePattern
```

You can control how many forks/warmup-iterations/iterations etc. are run using command line arguments, see
`java -jar kroxylicious-microbenchmarks/target/microbenchmarks.jar -h` for details.

### 3. Profilers

To get further insights into what is being run jmh can integrate with several profilers.
This [gist](https://gist.github.com/markrmiller/a04f5c734fad879f688123bc312c21af#file-jmh-profilers-md)
contains some instructions for async-profiler integration.

Also you can integrate with [hsdis](https://github.com/liuzhengyang/hsdis) and `perfasm` using `-prof perfasm`. Follow
the instructions on the `hsdis` github readme to build and install it in your JRE.

## JMH Reference

To get a better understanding of how to use JMH and some pitfalls, have a look at the
JMH [samples](https://github.com/openjdk/jmh/tree/master/jmh-samples/src/main/java/org/openjdk/jmh/samples)
