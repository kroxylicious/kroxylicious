/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProviderFactory;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of AWS KMS.
 */
@Plugin(configType = Config.class)
public class AwsKmsService implements KmsService<Config, String, AwsKmsEdek> {

    private final CredentialsProviderFactory credentialsProviderFactory;
    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile Config config;
    private CredentialsProvider credentialsProvider;

    public AwsKmsService() {
        this(CredentialsProviderFactory.DEFAULT);
    }

    @VisibleForTesting
    AwsKmsService(@NonNull CredentialsProviderFactory credentialsProviderFactory) {
        this.credentialsProviderFactory = Objects.requireNonNull(credentialsProviderFactory);
    }

    @Override
    public void initialize(@NonNull Config config) {
        Objects.requireNonNull(config);
        this.config = config;
        this.credentialsProvider = credentialsProviderFactory.createCredentialsProvider(config);
    }

    @NonNull
    @Override
    public AwsKms buildKms() {
        Objects.requireNonNull(config, "KMS service not initialized");
        return new AwsKms(config.endpointUrl(),
                credentialsProvider,
                config.region(),
                Duration.ofSeconds(20), config.sslContext());
    }

    @Override
    public void close() {
        Optional.ofNullable(credentialsProvider).ifPresent(CredentialsProvider::close);
    }
}
