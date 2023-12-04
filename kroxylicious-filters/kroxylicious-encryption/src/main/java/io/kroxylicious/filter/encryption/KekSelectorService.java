/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.kroxylicious.kms.service.Kms;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KekSelectorService
 * @param <C> The config type
 */
public interface KekSelectorService<C> {
    @NonNull
    TopicNameBasedKekSelector buildSelector(@NonNull Kms<?> kms, C options);

}