/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class ContributorTest {

    @Test
    void testDefaultFilterConfiguration() {
        Contributor<Object, Void, Context<Void>> contributor = new Contributor<>() {

            @NonNull
            @Override
            public String getTypeName() {
                return "a";
            }

            @NonNull
            @Override
            public Class<Void> getConfigType() {
                return Void.class;
            }

            @NonNull
            @Override
            public Object getInstance(Context<Void> context) {
                return 1;
            }
        };
        assertThat(contributor.getConfigType()).isEqualTo(Void.class);
        assertThat(contributor.requiresConfiguration()).isFalse();
    }

}
