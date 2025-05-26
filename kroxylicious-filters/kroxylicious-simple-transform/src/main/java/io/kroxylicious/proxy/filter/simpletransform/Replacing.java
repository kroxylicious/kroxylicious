/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import java.io.IOException;
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

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = Replacing.Config.class)
public class Replacing implements ByteBufferTransformationFactory<Replacing.Config> {
    public record Config(
                         @JsonProperty String charset,
                         @JsonProperty(required = true) String targetPattern,
                         @JsonProperty String replacementValue,
                         @JsonProperty Path pathToReplacementValue) {
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
        if (config.pathToReplacementValue != null) {
            if (!Files.isReadable(config.pathToReplacementValue)) {
                throw new PluginConfigurationException("Path: '" + config.pathToReplacementValue + "' is not readable. ");
            }
        }
    }

    @Override
    public Transformation createTransformation(Config configuration) {
        return new Transformation(configuration);
    }

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
                throw new RuntimeException(e);
            }
        }

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return ByteBuffer.wrap(new String(charset.decode(in).array()).replaceAll(targetPattern, replaceWith).getBytes(charset));
        }
    }
}
