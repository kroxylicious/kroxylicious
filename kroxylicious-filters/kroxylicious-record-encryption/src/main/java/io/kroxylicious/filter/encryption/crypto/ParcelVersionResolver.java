/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import java.util.Collection;
import java.util.List;

import io.kroxylicious.filter.encryption.common.AbstractResolver;
import io.kroxylicious.filter.encryption.config.ParcelVersion;

public class ParcelVersionResolver extends AbstractResolver<ParcelVersion, Parcel, ParcelVersionResolver> {

    public static final ParcelVersionResolver ALL = new ParcelVersionResolver(List.of(ParcelV1.INSTANCE));

    ParcelVersionResolver(Collection<Parcel> impls) {
        super(impls);
    }

    @Override
    protected ParcelVersionResolver newInstance(Collection<Parcel> values) {
        return new ParcelVersionResolver(values);
    }

}
