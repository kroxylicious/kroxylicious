/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplacingTest {

    private static final String TARGET_PATTERN = "AAA";
    private static final String REPLACEMENT_VALUE = "BBB";
    private static final Replacing.Config REMOVE_TARGET_CONFIG = new Replacing.Config(null, TARGET_PATTERN, "", null);
    private static final Replacing.Config REPLACE_TARGET_CONFIG = new Replacing.Config(null, TARGET_PATTERN, REPLACEMENT_VALUE, null);
    protected static final String TOPIC_NAME = "Topic-Alpha";
    protected static final byte[] EMPTY_STRING_BYTES = "".getBytes(StandardCharsets.UTF_8);
    private Replacing replacing;

    @BeforeEach
    void setUp() {
        replacing = new Replacing();
    }

    @Test
    void shouldRemoveTarget() {
        // Given
        Replacing.Transformation transformation = replacing.createTransformation(REMOVE_TARGET_CONFIG);
        ByteBuffer expected = ByteBuffer.wrap(EMPTY_STRING_BYTES);

        // When
        ByteBuffer resultBuffer = transformation.transform(TOPIC_NAME, ByteBuffer.wrap(TARGET_PATTERN.getBytes(StandardCharsets.UTF_8)));

        // Then
        assertThat(resultBuffer).isNotNull()
                .isEqualTo(expected);
    }

    @Test
    void shouldReplaceTarget() {
        // Given
        Replacing.Transformation transformation = replacing.createTransformation(REPLACE_TARGET_CONFIG);
        ByteBuffer expected = ByteBuffer.wrap(REPLACEMENT_VALUE.getBytes(StandardCharsets.UTF_8));

        // When
        ByteBuffer resultBuffer = transformation.transform(TOPIC_NAME, ByteBuffer.wrap(TARGET_PATTERN.getBytes(StandardCharsets.UTF_8)));

        // Then
        assertThat(resultBuffer).isNotNull()
                .describedAs("Expected to replace %s with %s but got '%s'", TARGET_PATTERN, REPLACEMENT_VALUE,
                        StandardCharsets.UTF_8.decode(resultBuffer.duplicate()).toString())
                .isEqualTo(expected);
    }

    @Test
    void shouldGetReplacementValueFromFile(@TempDir Path tempDir) throws IOException {
        // Given
        Path replacementPath = tempDir.resolve(TOPIC_NAME + ".txt");
        Files.writeString(replacementPath, REPLACEMENT_VALUE, StandardCharsets.UTF_8);
        Replacing.Config replaceFromFileConfig = new Replacing.Config(null, TARGET_PATTERN, null, replacementPath);
        Replacing.Transformation transformation = replacing.createTransformation(replaceFromFileConfig);
        ByteBuffer expected = ByteBuffer.wrap(REPLACEMENT_VALUE.getBytes(StandardCharsets.UTF_8));

        // When
        ByteBuffer resultBuffer = transformation.transform(TOPIC_NAME, ByteBuffer.wrap(TARGET_PATTERN.getBytes(StandardCharsets.UTF_8)));

        // Then
        assertThat(resultBuffer).isNotNull()
                .describedAs("Expected to replace %s with %s but got '%s'", TARGET_PATTERN, REPLACEMENT_VALUE,
                        StandardCharsets.UTF_8.decode(resultBuffer.duplicate()).toString())
                .isEqualTo(expected);
    }

    @Test
    void shouldTreatEmptyFileAsRemove(@TempDir Path tempDir) throws IOException {
        // Given
        Path replacementPath = tempDir.resolve(TOPIC_NAME + ".txt");
        Files.createFile(replacementPath);
        Replacing.Config replaceFromFileConfig = new Replacing.Config(null, TARGET_PATTERN, null, replacementPath);
        Replacing.Transformation transformation = replacing.createTransformation(replaceFromFileConfig);
        ByteBuffer expected = ByteBuffer.wrap(EMPTY_STRING_BYTES);

        // When
        ByteBuffer resultBuffer = transformation.transform(TOPIC_NAME, ByteBuffer.wrap(TARGET_PATTERN.getBytes(StandardCharsets.UTF_8)));

        // Then
        assertThat(resultBuffer).isNotNull()
                .describedAs("Expected to replace %s with %s but got '%s'", TARGET_PATTERN, "",
                        StandardCharsets.UTF_8.decode(resultBuffer.duplicate()).toString())
                .isEqualTo(expected);
    }

    @Test
    void shouldDefaultCharsetToUtf8() {
        // Given

        // When
        String actualCharset = REMOVE_TARGET_CONFIG.charset();

        // Then
        assertThat(actualCharset).isEqualTo(StandardCharsets.UTF_8.name());
    }

    @Test
    void shouldValidateCharsetIsValidName() {
        // Given
        Replacing.Config config = new Replacing.Config("kiwi", TARGET_PATTERN, null, null);

        // When
        // Then
        assertThatThrownBy(() -> replacing.validateConfiguration(config))
                .hasMessageContaining("Unsupported charset:")
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void shouldRejectInvalidCharsetName() {
        // Given
        Replacing.Config config = new Replacing.Config("", TARGET_PATTERN, null, null);

        // When
        // Then
        assertThatThrownBy(() -> replacing.validateConfiguration(config))
                .hasMessageContaining("Illegal charset name")
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void shouldRejectBothPathAndValue(@TempDir Path tempDir) throws IOException {
        // Given
        Path replacementPath = tempDir.resolve(TOPIC_NAME + ".txt");
        Files.createFile(replacementPath);
        Replacing.Config config = new Replacing.Config(null, TARGET_PATTERN, REPLACEMENT_VALUE, replacementPath);

        // When
        // Then
        assertThatThrownBy(() -> replacing.validateConfiguration(config))
                .hasMessageContaining("Both replacementValue and pathToReplacementValue are specified.")
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void shouldConvertNullReplacementToEmptyString() {
        // Given
        Replacing.Config config = new Replacing.Config(null, TARGET_PATTERN, null, null);

        // When
        Replacing.Transformation transformation = replacing.createTransformation(config);

        // Then
        assertThat(transformation)
                .extracting("replaceWith")
                .asInstanceOf(InstanceOfAssertFactories.STRING)
                .isBlank();
    }

    @Test
    void shouldRejectUnreadableFile(@TempDir Path tempDir) throws IOException {
        // Given
        Path replacementPath = tempDir.resolve(TOPIC_NAME + ".txt");
        Files.createFile(replacementPath);
        Set<PosixFilePermission> writeOnlyPermissions = Files.getPosixFilePermissions(replacementPath);
        writeOnlyPermissions.removeAll(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ));
        writeOnlyPermissions.add(PosixFilePermission.OWNER_WRITE); // make sure we can still cleanup
        Files.setPosixFilePermissions(replacementPath, writeOnlyPermissions);
        Replacing.Config config = new Replacing.Config(null, TARGET_PATTERN, null, replacementPath);

        // When
        // Then
        assertThatThrownBy(() -> replacing.validateConfiguration(config))
                .hasMessageContaining("is not readable")
                .isInstanceOf(PluginConfigurationException.class);
    }
}