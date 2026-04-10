/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.util.ArrayList;
import java.util.List;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.service.KmsException;

/**
 * Factory for the CredentialsProviders
 */
@FunctionalInterface
public interface CredentialsProviderFactory {

    CredentialsProvider createCredentialsProvider(Config config);

    CredentialsProviderFactory DEFAULT = config -> {
        var configured = new ArrayList<String>();
        if (config.longTermCredentialsProviderConfig() != null) {
            configured.add("longTermCredentials");
        }
        if (config.ec2MetadataCredentialsProviderConfig() != null) {
            configured.add("ec2MetadataCredentials");
        }
        if (config.webIdentityCredentialsProviderConfig() != null) {
            configured.add("webIdentityCredentials");
        }
        if (configured.size() != 1) {
            throw new KmsException(
                    "Config %s must define exactly one credential provider, found %s".formatted(config, List.copyOf(configured)));
        }
        return switch (configured.get(0)) {
            case "longTermCredentials" -> new LongTermCredentialsProvider(config.longTermCredentialsProviderConfig());
            case "ec2MetadataCredentials" -> new Ec2MetadataCredentialsProvider(config.ec2MetadataCredentialsProviderConfig());
            case "webIdentityCredentials" -> new WebIdentityCredentialsProvider(config.webIdentityCredentialsProviderConfig(), config.region());
            default -> throw new IllegalStateException("unreachable");
        };
    };
}
