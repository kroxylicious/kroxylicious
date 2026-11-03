/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

import java.util.Objects;

import io.kroxylicious.systemtests.api.FilterSpec;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * {@link FilterSpec} for the authorization filter.
 * Configures ACL-based authorization using a file containing the access control rules.
 *
 * @param aclFile path or reference to the ACL rules file
 */
public record AuthorizationFilterSpec(@NonNull String aclFile) implements FilterSpec {

    public AuthorizationFilterSpec {
        Objects.requireNonNull(aclFile, "aclFile must not be null");
    }
}