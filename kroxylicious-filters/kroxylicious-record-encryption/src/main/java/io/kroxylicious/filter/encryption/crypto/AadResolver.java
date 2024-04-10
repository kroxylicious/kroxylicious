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

public class AadResolver extends AbstractResolver<AadSpec, Aad, AadResolver> {

    private static final AadResolver ALL = new AadResolver(List.of(new AadNone()));

    AadResolver(Collection<Aad> impls) {
        super(impls);
    }

    public static AadResolver of(AadSpec... aadSpec) {
        return ALL.subset(aadSpec);
    }

    @Override
    protected AadResolver newInstance(Collection<Aad> values) {
        return new AadResolver(values);
    }
}
