/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.net.URI;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

public class FortanixDsmKmsTestKmsFacade extends AbstractFortanixDsmKmsTestKmsFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(FortanixDsmKmsTestKmsFacade.class);

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    @SuppressWarnings("resource")
    public void startKms() {
        LOGGER.trace("startKms");
        // testing against cloud, nothing to do
    }

    @Override
    public void stopKms() {
        LOGGER.trace("stopKms");
        // testing against cloud, nothing to do

    }

    @Override
    @NonNull
    protected URI getEndpointUrl() {
        return URI.create("https://api.uk.smartkey.io");
    }

    @Override
    protected String getApiKey() {
        var apiKey = System.getenv().get("FORTANIX_ADMIN_API_KEY");
        Objects.requireNonNull(apiKey);
        return apiKey;
    }
}
