/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results.cli;

///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21+
//DEPS com.fasterxml.jackson.core:jackson-core:${jackson.version}
//DEPS org.slf4j:slf4j-api:${slf4j.version}
//DEPS com.fasterxml.jackson.core:jackson-databind:${jackson.version}
//DEPS info.picocli:picocli:${picocli.version}
//DEPS com.github.spotbugs:spotbugs-annotations:${spotbugs-annotations.version}
//SOURCES ../RunMetadata.java
import java.nio.file.Path;
import java.util.concurrent.Callable;

import io.kroxylicious.benchmarks.results.RunMetadata;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI tool for collecting OpenMessaging Benchmark results and metadata.
 * <p>
 * Designed to be run via JBang with Maven resource filtering to resolve
 * dependency versions from the parent pom.
 */
@Command(name = "collect-results", mixinStandardHelpOptions = true, description = "Collect OpenMessaging Benchmark results and metadata.")
@SuppressWarnings({ "checkstyle:RegexpSinglelineJava", "java:S106", "java:S7476", "java:S125" }) // CLI tool that intentionally writes to System.out/err
public class CollectResults implements Callable<Integer> {

    @Option(names = "--generate-run-metadata", description = "Generate run-metadata.json in the given directory")
    private Path metadataDir;

    @Option(names = "--scenario", description = "Benchmark scenario name (e.g. baseline, proxy-no-filters)")
    private String scenario;

    @Option(names = "--workload", description = "OMB workload name (e.g. 1topic-1kb)")
    private String workload;

    @Option(names = "--target-rate", description = "Target producer rate in msg/sec for this probe")
    private Integer targetRate;

    @Option(names = "--warmup-duration-minutes", description = "Warmup phase duration in minutes")
    private Integer warmupDurationMinutes;

    @Option(names = "--test-duration-minutes", description = "Measurement phase duration in minutes")
    private Integer testDurationMinutes;

    @Option(names = "--benchmark-started-at", description = "ISO-8601 UTC timestamp when the benchmark job started")
    private String benchmarkStartedAt;

    @Option(names = "--benchmark-completed-at", description = "ISO-8601 UTC timestamp when the benchmark job completed")
    private String benchmarkCompletedAt;

    @Option(names = "--topics", description = "Number of topics in the workload")
    private Integer topics;

    @Option(names = "--partitions-per-topic", description = "Number of partitions per topic")
    private Integer partitionsPerTopic;

    @Option(names = "--message-size", description = "Message payload size in bytes")
    private Integer messageSize;

    @Option(names = "--producers-per-topic", description = "Number of producers per topic")
    private Integer producersPerTopic;

    @Option(names = "--consumer-per-subscription", description = "Number of consumers per subscription")
    private Integer consumerPerSubscription;

    @Option(names = "--proxy-pod", description = "Name of the proxy pod to query for resource limits")
    private String proxyPod;

    @Option(names = "--namespace", description = "Kubernetes namespace containing the proxy pod")
    private String namespace;

    public static void main(String... args) {
        int exitCode = execute(args);
        System.exit(exitCode);
    }

    static int execute(String... args) {
        return new CommandLine(new CollectResults()).execute(args);
    }

    @Override
    public Integer call() throws Exception {
        if (metadataDir != null) {
            RunMetadata.ProbeContext context = RunMetadata.ProbeContext.of(scenario, workload, targetRate)
                    .withPhases(warmupDurationMinutes, testDurationMinutes, benchmarkStartedAt, benchmarkCompletedAt)
                    .withWorkload(topics, partitionsPerTopic, messageSize, producersPerTopic, consumerPerSubscription)
                    .withProxy(namespace, proxyPod);
            RunMetadata.generate(metadataDir, context);
            System.out.println("Generated " + metadataDir.resolve("run-metadata.json"));
            return 0;
        }
        new CommandLine(this).usage(System.err);
        return 1;
    }
}
