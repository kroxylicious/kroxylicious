/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DocContent(@JsonProperty(required = true) String adoc) implements Documented {}
