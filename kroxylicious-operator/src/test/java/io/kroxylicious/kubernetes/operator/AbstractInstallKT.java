/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An abstract test that we can install the operator.
 * Abstract because this class only depends on kubectl.
 * It's not defined here how a Kube cluster is provided or how it knows about the images we're testing.
 */
abstract class AbstractInstallKT {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInstallKT.class);

    protected static void exec(String... args) throws IOException, InterruptedException {
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
            assertThat(process.exitValue()).describedAs(argList + " should have 0 exit code").isZero();
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
}
