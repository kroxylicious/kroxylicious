/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.util.Collection;
import java.util.List;

import io.kroxylicious.filter.encryption.common.AbstractResolver;
import io.kroxylicious.filter.encryption.config.CipherOverrideConfig;
import io.kroxylicious.filter.encryption.config.CipherSpec;

public class CipherSpecResolver extends AbstractResolver<CipherSpec, CipherManager, CipherSpecResolver> {

    public static CipherSpecResolver all(CipherOverrideConfig cipherOverrideConfig) {
        return new CipherSpecResolver(List.of(
                Aes.aes256gcm128(cipherOverrideConfig),
                ChaChaPoly.INSTANCE));
    }

    public CipherSpecResolver(Collection<CipherManager> impls) {
        super(impls);
    }

    @Override
    protected RuntimeException newException(String msg) {
        return new UnknownCipherSpecException(msg);
    }

    public static CipherSpecResolver of(CipherOverrideConfig cipherOverrideConfig, CipherSpec... cipherSpec) {
        return all(cipherOverrideConfig).subset(cipherSpec);
    }

    @Override
    protected CipherSpecResolver newInstance(Collection<CipherManager> values) {
        return new CipherSpecResolver(values);
    }
}
