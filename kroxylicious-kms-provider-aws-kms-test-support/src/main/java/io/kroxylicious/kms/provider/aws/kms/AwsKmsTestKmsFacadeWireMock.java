/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * AwsKmsTestKmsFacade class for WireMock
 */
public class AwsKmsTestKmsFacadeWireMock extends AbstractAwsKmsTestKmsFacade {

    private final URI Uri;
    private final Optional<String> region;
    private final Optional<String> accessKey;
    private final Optional<String> secretKey;

    public AwsKmsTestKmsFacadeWireMock(URI uri, Optional<String> region, Optional<String> accessKey, Optional<String> secretKey) {
        this.Uri = uri;
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    protected void startKms() {
    }

    @Override
    protected void stopKms() {
    }

    @Override
    @NonNull
    protected URI getAwsUrl() {
        return Uri;
    }

    @Override
    protected String getRegion() {
        return region.orElseThrow();
    }

    @Override
    protected String getSecretKey() {
        return accessKey.orElseThrow();
    }

    @Override
    protected String getAccessKey() {
        return secretKey.orElseThrow();
    }
}
