/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

public class FortanixDsmKmsTestKmsFacade extends AbstractFortanixDsmKmsTestKmsFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(FortanixDsmKmsTestKmsFacade.class);
    private static final String FORTANIX_ADMIN_API_KEY = System.getenv().get("FORTANIX_ADMIN_API_KEY");
    private static final String FORTANIX_API_ENDPOINT = System.getenv().get("FORTANIX_API_ENDPOINT");

    static final boolean AVAILABLE;
    static {
        AVAILABLE = FORTANIX_ADMIN_API_KEY != null && FORTANIX_API_ENDPOINT != null;
        if (!AVAILABLE) {
            LOGGER.info("FORTANIX_ADMIN_API_KEY and FORTANIX_API_ENDPOINT are not defined, the Fortanix KMS tests will be skipped");
        }
    }

    @Override
    public boolean isAvailable() {
        return AVAILABLE;
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
        return URI.create(FORTANIX_API_ENDPOINT);
    }

    @Override
    protected String getApiKey() {
        return FORTANIX_ADMIN_API_KEY;
    }

}
