/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import edu.umd.cs.findbugs.annotations.NonNull;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FilterFactoryTest {

    static class MyFilter implements ProduceRequestFilter {
        @Override
        public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
            return null;
        }
    }

    private class MyFactory<C> extends FilterFactory<MyFilter, C> {
        public MyFactory(Class<C> configType) {
            super(configType, MyFilter.class);
        }

        @NonNull
        @Override
        public MyFilter createFilter(FilterCreationContext context, C configuration) {
            return new MyFilter();
        }
    }

    @Test
    void filterType() {
        assertEquals(MyFilter.class, new MyFactory<>(Void.class).filterType());
    }

    @Test
    void configType() {
        assertEquals(Void.class, new MyFactory<>(Void.class).configType());
    }

    @Test
    void validateConfiguration() {
        MyFactory factoryWithNoConfig = new MyFactory<>(Void.class);
        factoryWithNoConfig.validateConfiguration(null);
        assertThrows(InvalidFilterConfigurationException.class, () -> {
            factoryWithNoConfig.validateConfiguration(new Object());
        });
        var factoryWithConfig = new MyFactory<>(String.class);
        factoryWithConfig.validateConfiguration("");
        assertThrows(InvalidFilterConfigurationException.class, () -> {
            factoryWithConfig.validateConfiguration(null);
        });
    }

}