/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalKroxyliciousOperatorExtensionImageResolutionTest {

    // ---- sanitiseImageRef ----

    @Test
    void sanitiseImageRefStripsCarriageReturnAndNewline() {
        assertThat(LocalKroxyliciousOperatorExtension.sanitiseImageRef("my-image:tag\r\n")).isEqualTo("my-image:tag");
    }

    @Test
    void sanitiseImageRefLeavesCleanRefUnchanged() {
        assertThat(LocalKroxyliciousOperatorExtension.sanitiseImageRef("my-image:tag")).isEqualTo("my-image:tag");
    }

    // ---- resolveOperandImage: env var branch ----

    @Test
    void resolveOperandImageReturnsEnvVarWhenSet() {
        // Given
        String envImage = "quay.io/kroxylicious/kroxylicious:latest";

        // When
        String result = LocalKroxyliciousOperatorExtension.resolveOperandImage(envImage, () -> null);

        // Then
        assertThat(result).isEqualTo(envImage);
    }

    @Test
    void resolveOperandImageSanitisesEnvVarImage() {
        // Given
        String envImage = "quay.io/kroxylicious/kroxylicious:latest\r\n";

        // When
        String result = LocalKroxyliciousOperatorExtension.resolveOperandImage(envImage, () -> null);

        // Then
        assertThat(result).isEqualTo("quay.io/kroxylicious/kroxylicious:latest");
    }

    @Test
    void resolveOperandImageIgnoresBlankEnvVar() {
        // Given: blank env var, valid properties supplier
        String expectedImage = "quay.io/kroxylicious/kroxylicious:from-props";
        Supplier<InputStream> propsSupplier = propertiesSupplier(expectedImage);

        // When
        String result = LocalKroxyliciousOperatorExtension.resolveOperandImage("   ", propsSupplier);

        // Then
        assertThat(result).isEqualTo(expectedImage);
    }

    // ---- resolveOperandImage: properties file branch ----

    @Test
    void resolveOperandImageReadsImageFromPropertiesFileWhenEnvVarAbsent() {
        // Given
        String expectedImage = "quay.io/kroxylicious/kroxylicious:1.2.3";

        // When
        String result = LocalKroxyliciousOperatorExtension.resolveOperandImage(null, propertiesSupplier(expectedImage));

        // Then
        assertThat(result).isEqualTo(expectedImage);
    }

    @Test
    void resolveOperandImageSanitisesPropertiesFileImage() {
        // Given
        String result = LocalKroxyliciousOperatorExtension.resolveOperandImage(null, propertiesSupplier("my-image:tag\r\n"));

        // Then
        assertThat(result).isEqualTo("my-image:tag");
    }

    // ---- resolveOperandImage: error branches ----

    @Test
    void resolveOperandImageThrowsWhenPropertiesFileNotFound() {
        // Given: supplier returns null (resource not on classpath)
        assertThatThrownBy(() -> LocalKroxyliciousOperatorExtension.resolveOperandImage(null, () -> null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not found on classpath");
    }

    @Test
    void resolveOperandImageThrowsWhenPropertyMissingFromFile() {
        // Given: properties file with no kroxylicious-image entry
        Supplier<InputStream> emptyProps = () -> new ByteArrayInputStream("other.property=value\n".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> LocalKroxyliciousOperatorExtension.resolveOperandImage(null, emptyProps))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'kroxylicious-image' not found");
    }

    @Test
    void resolveOperandImageThrowsWhenPropertyIsEmpty() {
        // Given: properties file with an empty kroxylicious-image value
        Supplier<InputStream> emptyValueProps = () -> new ByteArrayInputStream("kroxylicious-image=\n".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> LocalKroxyliciousOperatorExtension.resolveOperandImage(null, emptyValueProps))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'kroxylicious-image' not found");
    }

    @Test
    void resolveOperandImageWrapsIOExceptionAsIllegalStateException() {
        // Given: supplier returns a stream that throws on read
        Supplier<InputStream> brokenSupplier = () -> new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("disk error");
            }
        };

        assertThatThrownBy(() -> LocalKroxyliciousOperatorExtension.resolveOperandImage(null, brokenSupplier))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to read")
                .hasCauseInstanceOf(IOException.class);
    }

    // ---- helpers ----

    private static Supplier<InputStream> propertiesSupplier(String image) {
        String content = "kroxylicious-image=%s%n".formatted(image);
        return () -> new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
