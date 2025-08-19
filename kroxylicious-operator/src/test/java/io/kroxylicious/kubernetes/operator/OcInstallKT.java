/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.test.ShellUtils;

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
@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OcInstallKT#isEnvironmentValid", disabledReason = "environment not usable")
class OcInstallKT extends AbstractInstallKT {

    private static final Logger LOGGER = LoggerFactory.getLogger(OcInstallKT.class);

    private static final String IMAGE_NAME = OperatorInfo.fromResource().imageName();
    private static final String IMAGE_STREAM_NAME = "kroxylicious-operator";
    private static boolean loaded = false;

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        Assertions.setDescriptionConsumer(desc -> {
            LOGGER.info("Testing assumption: \"{}\"", desc);
        });

        LOGGER.info("Logging into oc registry");
        assumeThatCode(() -> ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "oc", "registry", "login", "--insecure=true"))
                .describedAs("oc registry login")
                .doesNotThrowAnyException();

        LOGGER.info("Importing {} into oc registry", IMAGE_NAME);
        ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "oc", "import-image", IMAGE_STREAM_NAME, "--from=" + IMAGE_NAME, "--confirm");
        loaded = true;
    }

    @AfterAll
    static void afterAll() throws IOException, InterruptedException {
        if (loaded) {
            LOGGER.info("Deleting imagestream {}", IMAGE_STREAM_NAME);
            ShellUtils.execValidate(ALWAYS_VALID, ALWAYS_VALID, "oc", "delete", "imagestream", IMAGE_STREAM_NAME);
        }
    }

    public static boolean isEnvironmentValid() {
        return ShellUtils.validateToolsOnPath("oc") && ShellUtils.execValidate(OcInstallKT::checkKubeContext, ALWAYS_VALID, "oc", "whoami", "-c")
                && testImageAvailable();
    }

    private static boolean checkKubeContext(Stream<String> lines) {
        return lines.anyMatch(line -> line.contains("crc-testing"));
    }
}
