/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.crypto;

import io.kroxylicious.filter.encryption.config.EncryptionConfigurationException;
import io.kroxylicious.filter.encryption.config.EncryptionVersion;
import io.kroxylicious.filter.encryption.config.WrapperVersion;

public interface WrapperVersionResolver {
    WrapperVersionResolver INSTANCE = new WrapperVersionResolver() {
        public Wrapper fromSpec(WrapperVersion version) {
            switch (version) {
                case V2:
                    return WrapperV2.INSTANCE;
                case V1_UNSUPPORTED:
                    return WrapperV1.INSTANCE;
            }
            throw new EncryptionConfigurationException("Unknown wrapper version " + version);
        }
    };

    Wrapper fromSpec(WrapperVersion version);

    static Wrapper fromEncryptionVersion(EncryptionVersion ev) {
        return WrapperVersionResolver.INSTANCE.fromSpec(ev.wrapperVersion());
    }
}
