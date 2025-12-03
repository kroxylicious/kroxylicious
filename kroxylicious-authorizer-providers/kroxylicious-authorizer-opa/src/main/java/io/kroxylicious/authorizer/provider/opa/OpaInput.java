/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class OpaInput {
    @JsonProperty("actions")
    OpaAction[] actions;

    @JsonProperty("subject")
    OpaSubject subject;

    public OpaInput(OpaAction[] actions, OpaSubject subject) {
        this.actions = actions;
        this.subject = subject;
    }

    public OpaAction[] actions() {
        return actions;
    }

    public OpaSubject subject() {
        return subject;
    }

    public static final class OpaAction {
        @JsonProperty("action")
        String action;

        @JsonProperty("resourceName")
        String resourceName;

        public OpaAction(String action, String resourceName) {
            this.action = action;
            this.resourceName = resourceName;
        }

        public String action() {
            return action;
        }

        public String resourceName() {
            return resourceName;
        }
    }

    public static final class OpaSubject {
        @JsonProperty("principals")
        OpaPrincipal[] principals;

        public OpaSubject(OpaPrincipal[] principals) {
            this.principals = principals;
        }

        public OpaPrincipal[] principals() {
            return principals;
        }
    }

    public static final class OpaPrincipal {
        @JsonProperty("name")
        String name;

        @JsonProperty("type")
        String type;

        public OpaPrincipal(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String name() {
            return name;
        }

        public String type() {
            return type;
        }
    }
}
