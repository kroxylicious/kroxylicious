/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.test.ShellUtils;

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

    private static final String IMAGE_NAME = OperatorInfo.fromResource().imageName();
    private static final String IMAGE_ARCHIVE = OperatorInfo.fromResource().imageArchive();

    @BeforeAll
    static void beforeAll() {
        LOGGER.info("Importing {} into kind", IMAGE_NAME);
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "kind", "load", "image-archive", IMAGE_ARCHIVE);
    }

    @AfterAll
    static void afterAll() {
        // TODO deleting image from kind would look something like `podman exec -it $(kind get clusters | head -1)-control-plane crictl rmi quay.io/kroxylicious/operator:0.13.0-SNAPSHOT`
        // but for that to work we need to know the container engine docker -vs- podman and the tag (which we don't currently have in the properties)
    }

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("kind") && validateKubeContext("kind-kind") && testImageAvailable();
    }
}
