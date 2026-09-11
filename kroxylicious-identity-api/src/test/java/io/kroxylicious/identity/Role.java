/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

/**
 * Test fixture: a non-singular principal type (may appear multiple times in a subject).
 */
record Role(String name) implements Principal {}
