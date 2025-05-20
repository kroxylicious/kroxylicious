/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = Replacing.Config.class)
public class Replacing implements ByteBufferTransformationFactory<Replacing.Config> {
    public record Config(@JsonProperty String charset, @JsonProperty(required = true) String findValue, @JsonProperty String replaceValue) {}

    @Override
    public void validateConfiguration(Config config) throws PluginConfigurationException {
        config = requireConfig(config);
        try {
            Charset.forName(config.charset());
        }
        catch (IllegalCharsetNameException e) {
            throw new PluginConfigurationException("Illegal charset name: '" + config.charset() + "'");
        }
        catch (UnsupportedCharsetException e) {
            throw new PluginConfigurationException("Unsupported charset: :" + config.charset() + "'");
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
            this.findPattern = config.findValue;
            this.replaceWith = Optional.ofNullable(config.replaceValue).orElse("");

        }

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return ByteBuffer.wrap(new String(charset.decode(in).array()).replaceAll(findPattern, replaceWith).getBytes(charset));
        }
    }
}
