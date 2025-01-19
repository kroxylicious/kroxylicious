/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

/**
 * Insecure trust manager that does no certificate checking.  Typically
 * used for development and testing use-cases.  Should not be used in
 * production.
 */
class InsecureTrustManager implements X509TrustManager {

    // suppressing sonar security warning as we are intentionally throwing security out the window
    @SuppressWarnings("java:S4830")
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
        // do nothing, the api is to throw if not trusted
    }

    // suppressing sonar security warning as we are intentionally throwing security out the window
    @SuppressWarnings("java:S4830")
    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {
        // do nothing, the api is to throw if not trusted
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
