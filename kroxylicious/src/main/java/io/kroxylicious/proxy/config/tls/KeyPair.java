/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.util.Optional;

import io.netty.handler.ssl.SslContextBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link KeyProvider} backed by a private-key/certificate pair expressed in PEM format.
 * <br/>
 * Note that support for PKCS-8 private keys is derived from the JDK.  PKCS-1 private keys are only supported if Bouncy Castle
 * is available on the classpath.
 *
 * @param privateKeyFile  location of a file containing the private key.
 * @param certificateFile location of a file containing the certificate and intermediates.
 * @param keyPassword     password used to protect the key within the storeFile or privateKeyFile
 */
@SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "Requires ability to consume file resources from arbitrary, user-specified, locations on the file-system.")
public record KeyPair(String privateKeyFile,
                      String certificateFile,
                      PasswordProvider keyPassword
) implements KeyProvider {

    @Override
    public SslContextBuilder forServer() {
        try {
            return SslContextBuilder.forServer(new File(certificateFile),
                    new File(privateKeyFile),
                    Optional.ofNullable(keyPassword).map(PasswordProvider::getProvidedPassword).orElse(null));
        }
        catch (Exception e) {
            throw new RuntimeException(
                    "Error building SSLContext. certificateFile : " + certificateFile + ", privateKeyFile: " + privateKeyFile + ", password present: " + (keyPassword != null),
                    e);
        }
    }
}
