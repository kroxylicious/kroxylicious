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

    /**
     * Creates the credentials provider defined by the given configuration.
     *
     * @param config KMS service configuration.
     * @return credentials provider.
     */
    CredentialsProvider createCredentialsProvider(Config config);

    /**
     * Default factory that creates the provider corresponding to the single credential
     * provider defined by the {@code credentials} node of the configuration.
     */
    CredentialsProviderFactory DEFAULT = config -> {
        var creds = config.credentials();
        if (creds == null) {
            throw new KmsException("Config %s must define a credential provider via the 'credentials' node".formatted(config));
        }
        var configured = new ArrayList<String>();
        if (creds.longTerm() != null) {
            configured.add("longTerm");
        }
        if (creds.ec2Metadata() != null) {
            configured.add("ec2Metadata");
        }
        if (creds.webIdentity() != null) {
            configured.add("webIdentity");
        }
        if (creds.podIdentity() != null) {
            configured.add("podIdentity");
        }
        if (configured.size() != 1) {
            throw new KmsException(
                    "Config must define exactly one credential provider, found %s".formatted(List.copyOf(configured)));
        }
        return switch (configured.getFirst()) {
            case "longTerm" -> new LongTermCredentialsProvider(creds.longTerm());
            case "ec2Metadata" -> new Ec2MetadataCredentialsProvider(creds.ec2Metadata());
            case "webIdentity" -> new WebIdentityCredentialsProvider(creds.webIdentity(), config.region());
            case "podIdentity" -> new PodIdentityCredentialsProvider(creds.podIdentity());
            default -> throw new IllegalStateException("unreachable");
        };
    };
}
