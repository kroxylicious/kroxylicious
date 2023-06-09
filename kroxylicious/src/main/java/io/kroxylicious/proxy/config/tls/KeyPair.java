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
 * A {@link KeyProvider} backed by a private-key/certificate pair.
 *
 * @param privateKeyFile  location of a file containing the private key. cannot be used if storeFile is specified
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
        return SslContextBuilder.forServer(new File(certificateFile),
                new File(privateKeyFile),
                Optional.ofNullable(keyPassword).map(PasswordProvider::getProvidedPassword).orElse(null));
    }
}
