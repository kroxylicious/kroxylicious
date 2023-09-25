/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class ContributorTest {

    @Test
    void testDefaultFilterConfiguration() {
        Contributor<Object, BaseConfig, Context<BaseConfig>> contributor = new Contributor<>() {

            @NonNull
            @Override
            public String getTypeName() {
                return "a";
            }

            @NonNull
            @Override
            public Class<BaseConfig> getConfigType() {
                return BaseConfig.class;
            }

            @NonNull
            @Override
            public Object getInstance(Context<BaseConfig> context) {
                return 1;
            }
        };
        assertThat(contributor.getConfigType()).isEqualTo(BaseConfig.class);
        assertThat(contributor.requiresConfiguration()).isFalse();
    }

}
