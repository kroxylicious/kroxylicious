/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import com.fasterxml.jackson.annotation.JsonProperty;

// This class maps the input expected by the Opa policy
// Should it be configurable by users?
public final class OpaInput {
    // TODO: extend to multiple subjects
    @JsonProperty("subject")
    String subject;

    @JsonProperty("actions")
    OpaAction[] actions;

    public OpaInput(String subject, OpaAction[] actions) {
        this.subject = subject;
        this.actions = actions;
    }

    public String subject() {
        return subject;
    }

    public OpaAction[] actions() {
        return actions;
    }

    public static final class OpaAction {
        @JsonProperty("operation")
        String operation;

        @JsonProperty("resource")
        String resource;

        public OpaAction(String operation, String resource) {
            this.operation = operation;
            this.resource = resource;
        }

        public String operation() {
            return operation;
        }

        public String resource() {
            return resource;
        }
    }
}
