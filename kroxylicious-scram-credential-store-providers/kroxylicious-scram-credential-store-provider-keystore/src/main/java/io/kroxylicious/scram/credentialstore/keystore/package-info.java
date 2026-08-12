/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Java KeyStore-backed implementation of the SCRAM credential store SPI.
 * <p>
 * This package provides {@link io.kroxylicious.scram.credentialstore.keystore.KeystoreScramCredentialStoreService},
 * a {@link io.kroxylicious.scram.credentialstore.ScramCredentialStoreService} plugin that loads
 * SCRAM credentials from a Java KeyStore file (JKS or PKCS12) at startup for fast, synchronous
 * lookups during SASL authentication.
 * </p>
 *
 * <h2>KeyStore Format</h2>
 * <p>
 * Credentials are stored as {@link javax.crypto.SecretKey} entries. Each entry:
 * </p>
 * <ul>
 *     <li>Uses the SHA-256 hex-encoded hash of the username as its alias</li>
 *     <li>Contains JSON-serialized {@link io.kroxylicious.scram.credentialstore.ScramCredential}
 *         data as the key bytes</li>
 * </ul>
 * <p>
 * Using a hash of the username as the alias prevents leaking usernames if the
 * KeyStore file is inspected directly.
 * </p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.scram.credentialstore.keystore;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;