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
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = Replacing.Config.class)
public class Replacing implements ByteBufferTransformationFactory<Replacing.Config> {
    public record Config(@JsonProperty String charset, @JsonProperty(required = true) String targetPattern, @JsonProperty String replacementValue,
                         @JsonProperty String replaceFrom) {}

    @Override
    public void validateConfiguration(Config config) throws PluginConfigurationException {
        config = requireConfig(config);
        String charsetName = Optional.ofNullable(config.charset()).orElse(StandardCharsets.UTF_8.name());
        try {
            Charset.forName(charsetName);
        }
        catch (IllegalCharsetNameException e) {
            throw new PluginConfigurationException("Illegal charset name: '" + charsetName + "'");
        }
        catch (UnsupportedCharsetException e) {
            throw new PluginConfigurationException("Unsupported charset: :" + charsetName + "'");
        }
        if (config.replacementValue != null && config.replaceFrom != null) {
            throw new PluginConfigurationException("Both replacementValue and replaceFrom are specified. MAKE UP YOUR MIND");
        }
        if (config.replaceFrom != null) {
            Path path = Path.of(config.replaceFrom);
            if (!Files.isReadable(path)) {
                throw new PluginConfigurationException("Path " + path + " is not readable. ");
            }
        }
    }

    @Override
    public Transformation createTransformation(Config configuration) {
        return new Transformation(configuration);
    }

    public static class Transformation implements ByteBufferTransformation {

        private final Charset charset;
        private final String findPattern;
        private final String replaceWith;

        Transformation(Config config) {
            this.charset = Charset.forName(Optional.ofNullable(config.charset()).orElse(StandardCharsets.UTF_8.name()));
            this.findPattern = config.targetPattern;
            try {
                if (config.replaceFrom != null) {
                    this.replaceWith = Files.readString(Path.of(config.replaceFrom));
                }
                else {
                    this.replaceWith = config.replaceFrom;
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return ByteBuffer.wrap(new String(charset.decode(in).array()).replaceAll(findPattern, replaceWith).getBytes(charset));
        }
    }
}
