/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.Path;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.Assumptions.assumeThatCode;

/**
 * An installation test which satisfies the requirements of {@link AbstractInstallKT} using {@code minikube}.
 * This test:
 * <ul>
 * <li>assumes minikube is running, so the test will be skipped if it's not.</li>
 * <li>loads the image using {@code minikube image load}.</li>
 * <li>cleans up the image using {@code minikube image rm}.</li>
 * </ul>
 */
@EnabledIf("io.kroxylicious.kubernetes.operator.MinikubeInstallKT#isEnvironmentValid")
class MinikubeInstallKT extends AbstractInstallKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MinikubeInstallKT.class);

    // These are normally set automatically by mvn
    private static final String IMAGE_ARCHIVE;
    private static final String IMAGE_NAME;

    static {
        OperatorInfo operatorInfo = OperatorInfo.fromResource();
        IMAGE_ARCHIVE = operatorInfo.imageArchive();
        IMAGE_NAME = operatorInfo.imageName();
    }

    private static boolean loaded = false;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        Assertions.setDescriptionConsumer(desc -> {
            LOGGER.info("Testing assumption: \"{}\"", desc);
        });

        assumeThat(Path.of(IMAGE_ARCHIVE))
                .describedAs("Container image archive %s must exist", IMAGE_ARCHIVE)
                .withFailMessage("Container image archive %s did not exist", IMAGE_ARCHIVE)
                .exists();

        LOGGER.info("Checking whether minikube is available");
        assumeThatCode(() -> exec("minikube"))
                .describedAs("minikube must be available on the path")
                .doesNotThrowAnyException();

        LOGGER.info("Checking whether minikube is running");
        assumeThatCode(() -> exec("minikube", "status"))
                .describedAs("minikube must be running")
                .doesNotThrowAnyException();

        LOGGER.info("Loading {} into minikube registry", IMAGE_ARCHIVE);
        exec("minikube", "image", "load", IMAGE_ARCHIVE);
        loaded = true;
    }

    @AfterAll
    static void afterAll() throws IOException, InterruptedException {
        if (loaded) {
            LOGGER.info("Removing {} from minikube registry", IMAGE_NAME);
            exec("minikube", "image", "rm", IMAGE_NAME);
        }
    }

    public static boolean isEnvironmentValid() throws IOException, InterruptedException {
        validateToolsOnPath("minikube");
        return validateKubeContext("minikube") && testImageAvailable();
    }
}
