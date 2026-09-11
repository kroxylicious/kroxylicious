/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * A {@link ByteBufferTransformationFactory} for a transformation which replaces
 * matches of a regular expression with a fixed replacement value.
 */
@Plugin(configType = Replacing.Config.class)
@DeprecatedPluginName(oldName = "io.kroxylicious.proxy.filter.simpletransform.Replacing", since = "0.19.0")
public class Replacing implements ByteBufferTransformationFactory<Replacing.Config> {

    /**
     * Creates a new factory.
     */
    public Replacing() {
        // empty
    }

    /**
     * The configuration for the {@link Replacing} transformation.
     * @param charset The name of the charset used to decode and re-encode the buffer. Defaults to UTF-8.
     * @param targetPattern The regular expression whose matches are replaced.
     * @param replacementValue The replacement value. Mutually exclusive with {@code pathToReplacementValue}.
     * @param pathToReplacementValue The path of a file containing the replacement value. Mutually exclusive with {@code replacementValue}.
     */
    public record Config(
                         @JsonProperty String charset,
                         @JsonProperty(required = true) String targetPattern,
                         @JsonProperty String replacementValue,
                         @JsonProperty Path pathToReplacementValue) {
        /**
         * Creates a new configuration.
         * @param charset The name of the charset used to decode and re-encode the buffer. Defaults to UTF-8 when null.
         * @param targetPattern The regular expression whose matches are replaced.
         * @param replacementValue The replacement value. Mutually exclusive with {@code pathToReplacementValue}.
         * @param pathToReplacementValue The path of a file containing the replacement value. Mutually exclusive with {@code replacementValue}.
         */
        public Config(@JsonProperty String charset, @JsonProperty(required = true) String targetPattern, @JsonProperty String replacementValue,
                      @JsonProperty Path pathToReplacementValue) {
            this.charset = Optional.ofNullable(charset).orElse(StandardCharsets.UTF_8.name());
            this.targetPattern = targetPattern;
            this.replacementValue = replacementValue;
            this.pathToReplacementValue = pathToReplacementValue;
        }
    }

    @Override
    public void validateConfiguration(Config config) throws PluginConfigurationException {
        config = requireConfig(config);
        try {
            Charset.forName(config.charset);
        }
        catch (IllegalCharsetNameException e) {
            throw new PluginConfigurationException("Illegal charset name: '" + config.charset + "'");
        }
        catch (UnsupportedCharsetException e) {
            throw new PluginConfigurationException("Unsupported charset: " + config.charset + "'");
        }
        if (config.replacementValue != null && config.pathToReplacementValue != null) {
            throw new PluginConfigurationException("Both replacementValue and pathToReplacementValue are specified. MAKE UP YOUR MIND");
        }
        if (config.pathToReplacementValue != null && !Files.isReadable(config.pathToReplacementValue)) {
            throw new PluginConfigurationException("Path: '" + config.pathToReplacementValue + "' is not readable. ");
        }
    }

    @Override
    public Transformation createTransformation(Config configuration) {
        return new Transformation(configuration);
    }

    /**
     * A {@link ByteBufferTransformation} which replaces matches of a regular expression with a replacement value.
     */
    public static class Transformation implements ByteBufferTransformation {

        private final Charset charset;
        private final String targetPattern;
        private final String replaceWith;

        Transformation(Config config) {
            this.charset = Charset.forName(Optional.ofNullable(config.charset()).orElse(StandardCharsets.UTF_8.name()));
            this.targetPattern = config.targetPattern;
            try {
                if (config.pathToReplacementValue != null) {
                    this.replaceWith = Files.readString(config.pathToReplacementValue);
                }
                else {
                    this.replaceWith = Objects.requireNonNullElse(config.replacementValue, "");
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return ByteBuffer.wrap(new String(charset.decode(in).array()).replaceAll(targetPattern, replaceWith).getBytes(charset));
        }
    }
}
