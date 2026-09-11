/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.util.Collection;
import java.util.List;

import io.kroxylicious.filter.encryption.common.AbstractResolver;
import io.kroxylicious.filter.encryption.config.AadSpec;

/**
 * A resolver of {@link AadSpec}s to their corresponding {@link Aad}s.
 */
public class AadResolver extends AbstractResolver<AadSpec, Aad, AadResolver> {

    private static final AadResolver ALL = new AadResolver(List.of(AadNone.INSTANCE));

    AadResolver(Collection<Aad> impls) {
        super(impls);
    }

    /**
     * Creates a resolver of the AADs corresponding to the given AAD specs.
     * @param aadSpec the AAD specs to resolve between.
     * @return a resolver of the AADs corresponding to the given AAD specs.
     */
    public static AadResolver of(AadSpec... aadSpec) {
        return ALL.subset(aadSpec);
    }

    @Override
    protected AadResolver newInstance(Collection<Aad> values) {
        return new AadResolver(values);
    }
}
