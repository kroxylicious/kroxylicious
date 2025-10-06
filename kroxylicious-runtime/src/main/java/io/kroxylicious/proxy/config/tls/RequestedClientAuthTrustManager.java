/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * A TrustManager that implements Kafka's "requested" client authentication semantics.
 * <br/>
 * This differs from Netty's OPTIONAL mode in that:
 * - Netty OPTIONAL: Client cert is optional and NOT validated if presented
 * - Kafka REQUESTED: Client cert is optional BUT must be valid if presented
 * <br/>
 * This TrustManager:
 * - Allows connections without client certificates (returns null accepted issuers to indicate optional)
 * - Validates client certificates if they ARE presented
 */
public class RequestedClientAuthTrustManager extends X509ExtendedTrustManager {

    private final X509TrustManager delegate;

    public RequestedClientAuthTrustManager(X509TrustManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // If a client certificate chain is presented, validate it
        if (chain != null && chain.length > 0) {
            delegate.checkClientTrusted(chain, authType);
        }
        // If no certificate is presented, allow the connection (requested, not required)
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        // If a client certificate chain is presented, validate it
        if (chain != null && chain.length > 0) {
            if (delegate instanceof X509ExtendedTrustManager x509ExtendedTrustManager) {
                x509ExtendedTrustManager.checkClientTrusted(chain, authType, socket);
            }
            else {
                delegate.checkClientTrusted(chain, authType);
            }
        }
        // If no certificate is presented, allow the connection (requested, not required)
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        // If a client certificate chain is presented, validate it
        if (chain != null && chain.length > 0) {
            if (delegate instanceof X509ExtendedTrustManager x509ExtendedTrustManager) {
                x509ExtendedTrustManager.checkClientTrusted(chain, authType, engine);
            }
            else {
                delegate.checkClientTrusted(chain, authType);
            }
        }
        // If no certificate is presented, allow the connection (requested, not required)
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkServerTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        if (delegate instanceof X509ExtendedTrustManager x509ExtendedTrustManager) {
            x509ExtendedTrustManager.checkServerTrusted(chain, authType, socket);
        }
        else {
            delegate.checkServerTrusted(chain, authType);
        }
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        if (delegate instanceof X509ExtendedTrustManager x509ExtendedTrustManager) {
            x509ExtendedTrustManager.checkServerTrusted(chain, authType, engine);
        }
        else {
            delegate.checkServerTrusted(chain, authType);
        }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        // Return empty array to indicate client auth is optional
        // Returning delegate.getAcceptedIssuers() would make it required
        return new X509Certificate[0];
    }
}
