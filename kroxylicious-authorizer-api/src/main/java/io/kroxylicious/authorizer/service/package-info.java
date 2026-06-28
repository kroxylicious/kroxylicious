/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * <p>An abstract API for making access control decisions about {@link io.kroxylicious.proxy.authentication.Subject Subjects}
 * wanting to perform {@link io.kroxylicious.authorizer.service.Action Actions}.</p>
 *
 * <p>The {@link io.kroxylicious.authorizer.service.AuthorizerService} interface is implemented by
 * service providers using the {@link java.util.ServiceLoader} mechanism.
 * An instance of {@link io.kroxylicious.authorizer.service.AuthorizerService} is a factory for
 * an {@link io.kroxylicious.authorizer.service.Authorizer}, which makes the access control decision.</p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.authorizer.service;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;