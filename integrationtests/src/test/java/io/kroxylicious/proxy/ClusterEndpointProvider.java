/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.sundr.builder.annotations.Buildable;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

@Buildable(editableEnabled = false)
public record ClusterEndpointProvider(String type, @JsonInclude(NON_EMPTY) Map<String, Object> config) {
}
