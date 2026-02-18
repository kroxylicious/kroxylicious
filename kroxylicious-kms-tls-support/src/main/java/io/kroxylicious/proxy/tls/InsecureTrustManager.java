/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Insecure trust manager that does no certificate checking.  Typically
 * used for development and testing use-cases.  Should not be used in
 * production.
 */
@SuppressWarnings("java:S4830") // InsecureTrustManager intentionally has a weak trust store
@SuppressFBWarnings("WEAK_TRUST_MANAGER") // InsecureTrustManager intentionally has a weak trust store
class InsecureTrustManager implements X509TrustManager {

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
        // do nothing, the api is to throw if not trusted
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {
        // do nothing, the api is to throw if not trusted
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
