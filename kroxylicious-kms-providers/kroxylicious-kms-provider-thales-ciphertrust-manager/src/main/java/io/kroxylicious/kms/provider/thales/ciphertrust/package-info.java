/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * KMS provider implementation for Thales CipherTrust Manager.
 * <p>
 * CipherTrust Manager is an enterprise key management solution from Thales that provides
 * centralized cryptographic key lifecycle management. This provider implements envelope
 * encryption using CTM's primitive cryptographic operations (random, encrypt, decrypt).
 * </p>
 * <p>
 * Unlike AWS KMS or HashiCorp Vault, CTM doesn't natively support envelope encryption,
 * so this implementation manually generates DEKs and wraps them using CTM's encryption API.
 * </p>
 *
 * <h2>Future Enhancements</h2>
 * <ul>
 * <li>Support CipherTrust Manager domains parameter (issue #4118)</li>
 * <li>Implement client credentials authentication (issue #4117)</li>
 * </ul>
 *
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kms.provider.thales.ciphertrust;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;