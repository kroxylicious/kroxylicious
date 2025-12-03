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
    OpaResultData result;

    public OpaResult() {
    }

    public OpaResultData result() {
        return result;
    }

    public static final class OpaResultData {
        @JsonProperty("allowed")
        @Nullable
        OpaActionResult[] allowed;

        @JsonProperty("denied")
        @Nullable
        OpaActionResult[] denied;

        public OpaResultData() {
        }

        public OpaActionResult[] allowed() {
            return allowed;
        }

        public OpaActionResult[] denied() {
            return denied;
        }
    }

    public static final class OpaActionResult {
        @JsonProperty("action")
        @Nullable
        String action;

        @JsonProperty("resourceName")
        @Nullable
        String resourceName;

        public OpaActionResult() {
        }

        public String action() {
            return action;
        }

        public String resourceName() {
            return resourceName;
        }
    }
}
