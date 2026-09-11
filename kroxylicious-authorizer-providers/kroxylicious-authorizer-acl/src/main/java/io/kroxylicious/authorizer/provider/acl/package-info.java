/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * An {@link io.kroxylicious.authorizer.service.Authorizer} implementation which makes authorization
 * decisions by evaluating a list of Access Control List (ACL) rules.
 *
 * <p>The rules can be built programmatically using the fluent API of
 * {@link io.kroxylicious.authorizer.provider.acl.AclAuthorizer}, or loaded from a rules file by
 * configuring the {@link io.kroxylicious.authorizer.provider.acl.AclAuthorizerService} plugin.</p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.authorizer.provider.acl;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;