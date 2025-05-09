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

import static org.assertj.core.api.Assertions.assertThat;

class InstallKT {

    @Test
    void shouldInstallFromYamlManifests() throws Exception {
        try {
            exec(List.of("kubectl",
                    "apply",
                    "-f",
                    "target/packaged/install"));

            exec(List.of("kubectl",
                    "wait",
                    "-n",
                    "kroxylicious-operator",
                    "--for=jsonpath={.status.readyReplicas}=1",
                    "--timeout=300s",
                    "deployment", "kroxylicious-operator"));

        }
        finally {
            exec(List.of("kubectl",
                    "delete",
                    "-f",
                    "target/packaged/install"));
        }
    }

    private static void exec(List<String> kubectl) throws IOException, InterruptedException {
        var out = Files.createTempFile(InstallKT.class.getSimpleName(), ".out");
        var err = Files.createTempFile(InstallKT.class.getSimpleName(), ".err");
        var process = new ProcessBuilder()
                .command(kubectl)
                .redirectOutput(out.toFile())
                .redirectError(err.toFile())
                .start();
        boolean exited = process.waitFor(5, TimeUnit.MINUTES);
        if (exited) {
            assertThat(process.exitValue()).describedAs(kubectl + " exited with error: " + Files.readString(err)).isZero();
        }
        else {
            process.destroyForcibly();
            throw new AssertionError("Process " + kubectl + " exited with code " + process.exitValue());
        }
    }
}
