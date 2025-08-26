/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doctools.asciidoc;

import java.nio.file.Path;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.asciidoctor.SafeMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class AdocConverterTest {

    @TempDir
    Path tempDir;

    @Test
    void adocToAdocFidelity() throws Exception {
        // Given
        try (Asciidoctor asciidoctor = Asciidoctor.Factory.create()) {
            asciidoctor.javaConverterRegistry().register(AdocConverter.class);

            var inputFile = Path.of(AdocConverterTest.class.getResource("/sample.adoc").toURI());
            // The AsciiDoc engine introduces HTML entities. There doesn't seem to be a way to control that.
            var expectedFile = Path.of(AdocConverterTest.class.getResource("/sample-with-html-entities.adoc").toURI());

            var options = Options.builder()
                    .safe(SafeMode.UNSAFE) // Required to write the output to temp location
                    .option(Options.TO_DIR, tempDir.toString())
                    .backend("adoc")
                    .build();

            // When
            asciidoctor.convertFile(
                    inputFile.toFile(),
                    options);

            // Then
            assertThat(tempDir.resolve("sample"))
                    .exists()
                    .hasSameTextualContentAs(expectedFile);
        }
    }

}