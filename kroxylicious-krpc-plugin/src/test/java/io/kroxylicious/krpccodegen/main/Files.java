/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.main;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import com.google.common.io.Resources;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class Files {
    static void assertFileHasExpectedContents(File file, String expectedFile) {
        try {
            String expected = Resources.asCharSource(
                    Objects.requireNonNull(com.google.common.io.Files.class.getClassLoader().getResource(expectedFile)), UTF_8).read();
            assertThat(file).content().isEqualTo(expected);
        }
        catch (IOException e) {
            // noinspection ResultOfMethodCallIgnored
            fail("Failed to read file: " + file.getAbsolutePath(), e);
        }
    }
}
