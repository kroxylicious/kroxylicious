/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An installation test which depends on Kind/{@code kind}.
 * This test:
 * <ul>
 * <li>assumes kind a kind cluster is available, so the test will be skipped if it's not.</li>
 * <li>loads the image using {@code kind load image-archive}.</li>
 *
 * </ul>
 */
@EnabledIf("io.kroxylicious.kubernetes.operator.KindInstallKT#isEnvironmentValid")
class KindInstallKT extends AbstractInstallKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KindInstallKT.class);

    // These are normally set automatically by mvn
    private static final String IMAGE_NAME = OperatorInfo.fromResource().imageName();
    private static final String IMAGE_ARCHIVE = OperatorInfo.fromResource().imageArchive();
    private static boolean loaded = false;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        LOGGER.info("Importing {} into kind", IMAGE_NAME);
        exec("kind",
                "load",
                "image-archive",
                IMAGE_ARCHIVE);
        loaded = true;
    }

    @AfterAll
    static void afterAll() throws IOException, InterruptedException {
        // if (loaded) {
        // TODO deleting image from kind would look something like `podman exec -it $(kind get clusters | head -1)-control-plane crictl rmi quay.io/kroxylicious/operator:0.13.0-SNAPSHOT`
        // but for that to work we need to know the container engine docker -vs- podman and the tag (which we don't currently have in the properties)
        // LOGGER.info("Deleting imagestream {}", IMAGE_STREAM_NAME);
        // exec("kind", "delete", "imagestream", IMAGE_STREAM_NAME);
        // }
    }

    public static boolean isEnvironmentValid() throws IOException, InterruptedException {
        return validateToolsOnPath("kind") && validateKubeContext("kind-kind") && testImageAvailable();
    }
}
