/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

/**
 * Entity types that the entity-isolation filter can isolate.
 * Mirrors the filter's own {@code EntityType} enum so that the test API module
 * does not require a compile-time dependency on the filter module.
 */
public enum IsolatedEntityType {
    TOPIC_NAME,
    GROUP_ID,
    TRANSACTIONAL_ID
}