/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.tls;

import java.net.Socket;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Insecure trust manager that does no certificate or hostname checking.
 * Typically used for development and testing use-cases. Should not be used in production.
 * <p>
 * Extends X509ExtendedTrustManager to disable hostname verification in java.net.http.HttpClient,
 * which doesn't respect SSLParameters.setEndpointIdentificationAlgorithm(null).
 * </p>
 */
@SuppressWarnings("java:S4830") // InsecureTrustManager intentionally has a weak trust store
@SuppressFBWarnings("WEAK_TRUST_MANAGER") // InsecureTrustManager intentionally has a weak trust store
class InsecureTrustManager extends X509ExtendedTrustManager {

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
        // do nothing, the api is to throw if not trusted
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
        // do nothing, the api is to throw if not trusted
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
        // do nothing, the api is to throw if not trusted
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
        // do nothing, the api is to throw if not trusted
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
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
