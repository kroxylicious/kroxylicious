/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.test.ShellUtils;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * An abstract test that we can install the operator.
 * Abstract because this class only depends on kubectl.
 * It's not defined here how a Kube cluster is provided or how it knows about the images we're testing.
 */
abstract class AbstractInstallKT {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInstallKT.class);
    static final Function<Stream<String>, Boolean> ALWAYS_VALID = lines -> true;

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
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "apply", "-f", "target/packaged/install");

            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "wait", "-n", "kroxylicious-operator", "--for=jsonpath={.status.readyReplicas}=1",
                    "--timeout=300s", "deployment", "kroxylicious-operator");
            LOGGER.info("Operator deployment became ready");
        }
        finally {
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kubectl", "delete", "-f", "target/packaged/install");
        }
    }

    static boolean validateKubeContext(String expectedContext) {
        return ShellUtils.execValidate(lines -> lines.anyMatch(line -> line.contains(expectedContext)), ALWAYS_VALID, "kubectl", "config", "current-context");
    }
}
