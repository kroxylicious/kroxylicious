/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

public final class OpaResult {
    @JsonProperty("result")
    @Nullable
    Boolean result;

    public OpaResult() {
    }

    public Boolean result() {
        return result;
    }
}
