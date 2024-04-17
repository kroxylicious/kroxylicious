/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.time.Duration;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of AWS KMS.
 */
@Plugin(configType = Config.class)
public class AwsKmsKmsService implements KmsService<Config, String, AwsKmsEdek> {

    @NonNull
    @Override
    public AwsKmsKms buildKms(Config options) {
        return new AwsKmsKms(options.endpointUrl(), options.accessKey().getProvidedPassword(), options.secretKey().getProvidedPassword(), options.region(),
                Duration.ofSeconds(20), options.sslContext());
    }

}
