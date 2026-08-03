/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Pluggable SASL credential store API.
 * <p>
 * This package provides an abstraction for SCRAM credential storage, allowing
 * SASL termination filters to authenticate clients against various backing stores
 * such as files, databases, LDAP directories, or external identity providers.
 * </p>
 *
 * <h2>Core Interfaces</h2>
 * <ul>
 *     <li>{@link io.kroxylicious.scram.credentialstore.ScramCredentialStoreService} -
 *         Service interface for discovering and initialising credential stores</li>
 *     <li>{@link io.kroxylicious.scram.credentialstore.ScramCredentialStore} -
 *         Store interface for looking up credentials</li>
 * </ul>
 *
 * <h2>Data Types</h2>
 * <ul>
 *     <li>{@link io.kroxylicious.scram.credentialstore.ScramCredential} -
 *         Immutable SCRAM credential with validation</li>
 * </ul>
 *
 * <h2>Exception Handling</h2>
 * <ul>
 *     <li>{@link io.kroxylicious.scram.credentialstore.CredentialLookupException} -
 *         Base exception for lookup failures</li>
 *     <li>{@link io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException} -
 *         Service is unavailable</li>
 *     <li>{@link io.kroxylicious.scram.credentialstore.CredentialServiceTimeoutException} -
 *         Operation timed out</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Initialise service
 * ScramCredentialStoreService<MyConfig> service = ...;
 * service.initialize(config);
 *
 * // Build credential store
 * ScramCredentialStore store = service.buildCredentialStore();
 *
 * // Look up credential asynchronously
 * CompletionStage<ScramCredential> stage = store.lookupCredential("alice");
 * stage.thenAccept(credential -> {
 *     if (credential != null) {
 *         // Authenticate using credential
 *     } else {
 *         // User not found
 *     }
 * }).exceptionally(error -> {
 *     // Handle service failure
 *     return null;
 * });
 *
 * // Clean up
 * service.close();
 * }</pre>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.scram.credentialstore.keystore;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;