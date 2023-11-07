/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.netty.handler.ssl.SslContextBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link KeyProvider} backed by a private-key/certificate pair expressed in PEM format.
 * <br/>
 * Note that support for PKCS-8 private keys is derived from the JDK.  PKCS-1 private keys are only supported if Bouncy Castle
 * is available on the classpath.
 *
 * @param privateKeyFile      location of a file containing the private key.
 * @param certificateFile     location of a file containing the certificate and intermediates.
 * @param keyPasswordProvider provider for the privateKeyFile password or null if key does not require a password
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "The paths provide the location for key material which may exist anywhere on the file-system. Paths are provided by the user in the administrator role via Kroxylicious configuration. ")
public record KeyPair(String privateKeyFile,
                      String certificateFile,
                      @JsonProperty(value = "keyPassword") PasswordProvider keyPasswordProvider)
        implements KeyProvider {

    @Override
    public SslContextBuilder forServer() {
        try {
            return SslContextBuilder.forServer(new File(certificateFile),
                    new File(privateKeyFile),
                    Optional.ofNullable(keyPasswordProvider).map(PasswordProvider::getProvidedPassword).orElse(null));
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Error building SSLContext. certificateFile : " + certificateFile + ", privateKeyFile: " + privateKeyFile + ", password present: "
                            + (keyPasswordProvider != null),
                    e);
        }
    }
}
