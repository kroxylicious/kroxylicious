/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authentication;

/**
 * A principal identifying an authenticated client.
 * It is recommended, but not required, to use this principal to represent clients that have authenticated via
 * TLS or SASL.
 * @param name The name of the user.
 */
public record User(String name) implements Principal {}
