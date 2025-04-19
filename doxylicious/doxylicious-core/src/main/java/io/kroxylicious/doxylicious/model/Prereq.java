/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

public record Prereq(
                     @JsonProperty(required = false) @Nullable String adoc,
                     @JsonProperty(required = true) String ref)
        implements Documented {}
