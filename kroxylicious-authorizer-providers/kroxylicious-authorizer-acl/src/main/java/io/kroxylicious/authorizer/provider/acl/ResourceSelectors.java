/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.Objects;

/**
 * The resource names an {@link AclAuthorizer} allows a subject to perform an operation on, given as
 * the name selectors of the allow and deny rules that apply. A name is allowed if an allow rule
 * selects it and no deny rule does, exactly as {@link AclAuthorizer#authorize} decides.
 *
 * <p>This lets callers that list resources filter them consistently with {@code authorize}, for
 * example by translating the selectors into a database query, so that paging and counts are exact.</p>
 *
 * @param allowed the names selected by the allow rules that apply.
 * @param denied the names selected by the deny rules that apply.
 */
public record ResourceSelectors(ResourceNameSelectors allowed, ResourceNameSelectors denied) {

    /**
     * Constructs a ResourceSelectors.
     * @param allowed the names selected by the allow rules that apply.
     * @param denied the names selected by the deny rules that apply.
     */
    public ResourceSelectors {
        Objects.requireNonNull(allowed);
        Objects.requireNonNull(denied);
    }

    /**
     * Returns true if the operation is allowed on the resource with this name.
     * @param name a resource name.
     * @return true if the operation is allowed on the resource with this name.
     */
    public boolean allows(String name) {
        return allowed.matches(name) && !denied.matches(name);
    }
}
