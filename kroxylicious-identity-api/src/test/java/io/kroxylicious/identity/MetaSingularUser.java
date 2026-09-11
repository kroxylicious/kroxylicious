/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Test fixture: a principal type that is singular via a meta-annotation, mirroring how the
 * deprecated {@code io.kroxylicious.proxy.authentication.Unique} carries {@link SingularPrincipal}.
 */
@MetaSingularUser.CustomUnique
record MetaSingularUser(String name) implements Principal {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @SingularPrincipal
    @interface CustomUnique {
    }
}
