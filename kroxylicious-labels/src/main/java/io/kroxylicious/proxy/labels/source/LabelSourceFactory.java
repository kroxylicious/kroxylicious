/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.labels.source;

import edu.umd.cs.findbugs.annotations.UnknownNullness;

public interface LabelSourceFactory<C, I> {

    @UnknownNullness
    I initialize(LabelSourceFactoryContext context, @UnknownNullness C config);

    LabelSource create(I initializationData);

    default void close(@UnknownNullness I initializationData) {
    }
}
