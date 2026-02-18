/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

///usr/bin/env jbang "$0" "$@" ; exit $?
//JAVA 21+
//DEPS com.fasterxml.jackson.core:jackson-core:${jackson.version}
//DEPS com.fasterxml.jackson.core:jackson-databind:${jackson.version}
//DEPS info.picocli:picocli:${picocli.version}
import java.nio.file.Path;
import java.util.concurrent.Callable;

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
@SuppressWarnings({ "checkstyle:RegexpSinglelineJava" }) // CLI tool that intentionally writes to System.out/err
public class CollectResults implements Callable<Integer> {

    @Option(names = "--generate-run-metadata", description = "Generate run-metadata.json in the given directory")
    private Path metadataDir;

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
            RunMetadata.generate(metadataDir);
            System.out.println("Generated " + metadataDir.resolve("run-metadata.json"));
            return 0;
        }
        new CommandLine(this).usage(System.err);
        return 1;
    }
}
