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
    void testDefaultConfigDefinition() {
        Contributor<Object, Context> contributor = new Contributor<>() {

            @NonNull
            @Override
            public String getTypeName() {
                return "a";
            }

            @NonNull
            @Override
            public Object getInstance(Context context) {
                return 1;
            }
        };
        assertThat(contributor.getConfigDefinition()).isEqualTo(new ConfigurationDefinition(BaseConfig.class, false));
    }

}
