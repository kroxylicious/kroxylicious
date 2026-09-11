/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Resource check result
 *
 * @param <T> custom resource type
 * @param resource null if check passed, else a resource with status updated to reflect problems
 * @param referents contains any referents if check passed and resource had refs
 */
public record ResourceCheckResult<T extends CustomResource<?, ?>>(@Nullable T resource, List<? extends HasMetadata> referents) {}
