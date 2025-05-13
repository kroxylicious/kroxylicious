/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assumptions.assumeThatCode;

/**
 * An installation test which depends on CRC/{@code oc}.
 * This test:
 * <ul>
 * <li>assumes CRC is running, so the test will be skipped if it's not.</li>
 * <li>loads the image using {@code oc import-image}.</li>
 * <li>cleans up the image using {@code oc delete imagestream}.</li>
 *
 * </ul>
 */
class OcInstallKT extends AbstractInstallKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcInstallKT.class);

    // These are normally set automatically by mvn
    private static final String IMAGE_NAME = OperatorInfo.fromResource().imageName();
    private static final String IMAGE_STREAM_NAME = "kroxylicious-operator";
    private static boolean loaded = false;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        Assertions.setDescriptionConsumer(desc -> {
            LOGGER.info("Testing assumption: \"{}\"", desc);
        });

        LOGGER.info("Checking whether oc is available");
        assumeThatCode(() -> exec("oc"))
                .describedAs("oc must be available on the path")
                .doesNotThrowAnyException();

        LOGGER.info("Checking whether oc is logged in");
        assumeThatCode(() -> exec("oc", "whoami"))
                .describedAs("oc must be logged in")
                .doesNotThrowAnyException();

        LOGGER.info("Logging into oc registry");
        assumeThatCode(() -> exec("oc", "registry", "login", "--insecure=true"))
                .describedAs("oc registry login")
                .doesNotThrowAnyException();

        LOGGER.info("Importing {} into oc registry", IMAGE_NAME);
        exec("oc",
                "import-image",
                IMAGE_STREAM_NAME,
                "--from=" + IMAGE_NAME,
                "--confirm");
        loaded = true;
    }

    @AfterAll
    static void afterAll() throws IOException, InterruptedException {
        if (loaded) {
            LOGGER.info("Deleting imagestream {}", IMAGE_STREAM_NAME);
            exec("oc", "delete", "imagestream", IMAGE_STREAM_NAME);
        }
    }

}
