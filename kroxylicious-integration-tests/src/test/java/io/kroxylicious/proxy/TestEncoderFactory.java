/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.nio.ByteBuffer;

import io.kroxylicious.proxy.internal.filter.ByteBufferTransformation;
import io.kroxylicious.proxy.internal.filter.ByteBufferTransformationFactory;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

@Plugin(configType = Void.class)
public class TestEncoderFactory implements ByteBufferTransformationFactory<Void> {

    @Override
    public void validateConfiguration(Void config) throws PluginConfigurationException {

    }

    @Override
    public TestEncoder createTransformation(Void configuration) {
        return new TestEncoder();
    }

    public static class TestEncoder implements ByteBufferTransformation {

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return FilterIT.encode(topicName, in);
        }
    }

}
