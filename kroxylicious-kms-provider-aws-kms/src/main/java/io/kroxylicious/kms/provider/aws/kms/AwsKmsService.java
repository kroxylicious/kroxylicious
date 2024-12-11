/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Objects;

import javax.net.ssl.SSLContext;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.JdkTls;
import io.kroxylicious.kms.provider.aws.kms.config.SslConfigurationException;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of AWS KMS.
 */
@Plugin(configType = Config.class)
public class AwsKmsService implements KmsService<Config, String, AwsKmsEdek> {

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile Config config;

    @NonNull
    public static SSLContext sslContext(Tls tls) {
        try {
            if (tls == null) {
                return SSLContext.getDefault();
            }
            else {
                return new JdkTls(tls).sslContext();
            }
        }
        catch (NoSuchAlgorithmException e) {
            throw new SslConfigurationException(e);
        }
    }

    @Override
    public void initialize(@NonNull Config config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @NonNull
    @Override
    public AwsKms buildKms() {
        Objects.requireNonNull(config, "KMS service not initialized");
        SSLContext sslContext = sslContext(config.tls());
        return new AwsKms(config.endpointUrl(),
                config.accessKey().getProvidedPassword(),
                config.secretKey().getProvidedPassword(),
                config.region(),
                Duration.ofSeconds(20), sslContext);
    }
}
