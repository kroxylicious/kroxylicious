/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assumptions.assumeThatCode;

/**
 * An abstract test that we can install the operator.
 * Abstract because this class only depends on kubectl.
 * It's not defined here how a Kube cluster is provided or how it knows about the images we're testing.
 */
abstract class AbstractInstallKT {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInstallKT.class);
    static final Function<Stream<String>, Boolean> ALWAYS_VALID = lines -> true;

    protected static void exec(String... args) throws IOException, InterruptedException {
        execValidate(ALWAYS_VALID, ALWAYS_VALID, args);
    }

    protected static boolean execValidate(Function<Stream<String>, Boolean> stdOutValidator, Function<Stream<String>, Boolean> stdErrValidator, String... args)
            throws IOException, InterruptedException {
        List<String> argList = List.of(args);
        LOGGER.info("Executing '{}'", String.join(" ", argList));
        var out = Files.createTempFile(AbstractInstallKT.class.getSimpleName(), ".out");
        var err = Files.createTempFile(AbstractInstallKT.class.getSimpleName(), ".err");
        var process = new ProcessBuilder()
                .command(argList)
                .redirectOutput(out.toFile())
                .redirectError(err.toFile())
                .start();
        boolean exited = process.waitFor(5, TimeUnit.MINUTES);
        if (exited) {
            if (process.exitValue() != 0 || LOGGER.isDebugEnabled()) {
                String description = ("'%s' exited with value: %d%n"
                        + "standard error output:%n"
                        + "---%n"
                        + "%s---%n"
                        + "standard output:%n"
                        + "---%n"
                        + "%s---").formatted(
                                argList,
                                process.exitValue(),
                                Files.readString(err),
                                Files.readString(out));
                LOGGER.info(description);
            }
            assertThat(process.exitValue()).describedAs(argList + " should have 0 exit code").isZero();
            var rtnValue = stdOutValidator.apply(Files.lines(out));
            rtnValue &= stdErrValidator.apply(Files.lines(err));
            return rtnValue;
        }
        else {
            process.destroy();
            boolean killed = process.waitFor(5, TimeUnit.SECONDS);
            if (!killed) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
            throw new AssertionError("Process " + argList + " did not complete within timeout");
        }
    }

    static boolean isToolOnPath(String tool) {
        LOGGER.info("Checking whether {} is available", tool);
        assumeThatCode(() -> exec(tool))
                .describedAs(tool + " must be available on the path")
                .doesNotThrowAnyException();
        return true;
    }

    static boolean testImageAvailable() {
        String imageArchive = OperatorInfo.fromResource().imageArchive();
        assumeThat(Path.of(imageArchive))
                .describedAs("Container image archive %s must exist", imageArchive)
                .withFailMessage("Container image archive %s did not exist", imageArchive)
                .exists();
        return true;
    }

    @Test
    void shouldInstallFromYamlManifests() throws Exception {
        try {
            AbstractInstallKT.exec("kubectl",
                    "apply",
                    "-f",
                    "target/packaged/install");

            AbstractInstallKT.exec("kubectl",
                    "wait",
                    "-n",
                    "kroxylicious-operator",
                    "--for=jsonpath={.status.readyReplicas}=1",
                    "--timeout=300s",
                    "deployment", "kroxylicious-operator");
            LOGGER.info("Operator deployment became ready");
        }
        finally {
            AbstractInstallKT.exec("kubectl",
                    "delete",
                    "-f",
                    "target/packaged/install");
        }
    }

    static void validateToolsOnPath(String... additionalTools) {
        Set<String> tools = Sets.newLinkedHashSet(additionalTools);
        tools.add("kubectl");
        Set<String> validTools = tools.stream().filter(AbstractInstallKT::isToolOnPath).collect(Collectors.toSet());
        Assertions.assertThat(validTools).containsExactlyInAnyOrderElementsOf(tools);
    }

    static boolean validateKubeContext(String expectedContext) throws IOException, InterruptedException {
        return execValidate(lines -> lines.anyMatch(line -> line.contains(expectedContext)), ALWAYS_VALID, "kubectl", "config", "current-context");
    }
}
