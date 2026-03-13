/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.metrics;

import java.util.Map;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * @param objectScopeMapping A mapping from objectRef scope to meter tag key.
 * If an objectRef scope is not present in the {@code objectScopeMapping} there
 * will not be a corresponding tag in the associated meter tags.
 * If an objectRef scope is present the meter tag will be the mapping's value.
 * This can be useful to avoid objectRef's with a large number of entries from in meter's
 * with a large number of tags (a.k.a. cardinality explosion), and to ensure that tags
 * are named consistently.
 */
public record MetricsEmitterConfig(@Nullable Map<String, String> objectScopeMapping) {}
