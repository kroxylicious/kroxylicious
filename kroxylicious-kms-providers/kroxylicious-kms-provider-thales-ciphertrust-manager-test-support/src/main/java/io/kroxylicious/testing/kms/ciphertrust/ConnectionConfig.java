/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Connection configuration for CipherTrust Manager.
 *
 * @param username username for password authentication, or null for client cert auth
 * @param password password for password authentication, or null for client cert auth
 * @param clientId client ID for client certificate authentication, or null for password auth
 * @param tls TLS configuration, or null if not needed
 */
record ConnectionConfig(
                        @Nullable String username,
                        @Nullable String password,
                        @Nullable String clientId,
                        @Nullable Tls tls) {}
