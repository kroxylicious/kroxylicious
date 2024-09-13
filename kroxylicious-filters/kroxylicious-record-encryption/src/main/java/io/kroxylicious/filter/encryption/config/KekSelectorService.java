/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import io.kroxylicious.kms.service.Kms;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KekSelectorService
 * @param <C> The config type
 * @param <K> the type of key
 */
public interface KekSelectorService<C, K> {
    @NonNull
    TopicNameBasedKekSelector<K> buildSelector(
            @NonNull
            Kms<K, ?> kms,
            C options
    );

}
