/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.thales;

import io.kroxylicious.testing.kms.ciphertrust.CipherTrustTestKmsFacade;

/**
 * CipherTrust Manager facade for the system tests.
 * The base facade falls back to Mock, so this facade is therefore only available when a
 * real CipherTrust Manager instance has been configured via {@value #ENDPOINT_ENV},
 * so that the tests are skipped rather than failing in a confusing way.
 */
public class KubeCipherTrustTestKmsFacade extends CipherTrustTestKmsFacade {

    static final String ENDPOINT_ENV = "KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT";

    @Override
    public boolean isAvailable() {
        var endpoint = System.getenv(ENDPOINT_ENV);
        return endpoint != null && !endpoint.isBlank();
    }
}
