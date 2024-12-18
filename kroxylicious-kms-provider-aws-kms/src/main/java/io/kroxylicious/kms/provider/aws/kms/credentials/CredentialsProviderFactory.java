/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.LongTermCredentialsProviderConfig;
import io.kroxylicious.kms.service.KmsException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for the CredentialsProviders
 */
@FunctionalInterface
public interface CredentialsProviderFactory {

    @NonNull
    CredentialsProvider createCredentialsProvider(@NonNull Config config);

    CredentialsProviderFactory DEFAULT = new CredentialsProviderFactory() {
        @NonNull
        @Override
        public CredentialsProvider createCredentialsProvider(@NonNull Config config) {
            var configException = new KmsException("Config %s must define exactly one credential provider".formatted(config));
            if (config.longTermCredentialsProviderConfig() != null) {
                if (config.ec2MetadataCredentialsProviderConfig() != null || config.secretKey() != null || config.accessKey() != null) {
                    throw configException;
                }
                return new LongTermCredentialsProvider(config.longTermCredentialsProviderConfig());
            }
            else if (config.ec2MetadataCredentialsProviderConfig() != null) {
                if (config.secretKey() != null || config.accessKey() != null) {
                    throw configException;
                }
                return new Ec2MetadataCredentialsProvider(config.ec2MetadataCredentialsProviderConfig());
            }
            else if (config.accessKey() != null && config.secretKey() != null) {
                return new LongTermCredentialsProvider(new LongTermCredentialsProviderConfig(config.accessKey(), config.secretKey()));
            }
            else {
                throw configException;
            }
        }
    };
}
