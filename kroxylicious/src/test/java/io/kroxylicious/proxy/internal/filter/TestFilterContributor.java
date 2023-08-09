/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.service.BaseContributor;

public class TestFilterContributor extends BaseContributor<KrpcFilter> implements FilterContributor {

    public static final String NO_CONFIG_REQUIRED_TYPE_NAME = "NoConfigRequired";
    public static final String CONFIG_REQUIRED_TYPE_NAME = "ConfigRequired";
    public static final BaseContributorBuilder<KrpcFilter> FILTERS = BaseContributor.<KrpcFilter> builder()
            .add(NO_CONFIG_REQUIRED_TYPE_NAME, NoConfigFilter::new)
            .add(CONFIG_REQUIRED_TYPE_NAME, ConfigRequiredFilter.Config.class, config -> new ConfigRequiredFilter(config.property));

    public TestFilterContributor() {
        super(FILTERS);
    }

    public static class NoConfigFilter implements RequestFilter {
        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public static class ConfigRequiredFilter implements RequestFilter {

        @SuppressWarnings("FieldCanBeLocal")
        private final String property;

        ConfigRequiredFilter(String property) {
            this.property = property;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
            return CompletableFuture.completedFuture(null);
        }

        public static final class Config extends BaseConfig {
            private final String property;

            public Config(String property) {
                this.property = property;
            }

            public String property() {
                return property;
            }
        }
    }
}
