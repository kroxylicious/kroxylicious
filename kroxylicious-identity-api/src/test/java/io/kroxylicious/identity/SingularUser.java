/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

/**
 * Test fixture: a singular principal type (annotated with {@link SingularPrincipal} directly).
 */
@SingularPrincipal
record SingularUser(String name) implements Principal {}
