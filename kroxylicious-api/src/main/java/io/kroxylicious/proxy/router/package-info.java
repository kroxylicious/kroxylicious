/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * The router API.
 *
 * <p>A {@link io.kroxylicious.proxy.router.Router} is a plugin that decides which
 * {@linkplain io.kroxylicious.proxy.router.RouterContext#sendRequestToNode route}
 * should handle a given incoming Kafka request. Routes lead to receivers &mdash;
 * either a backing Kafka cluster or another {@code Router} &mdash; forming a
 * directed acyclic graph (DAG).</p>
 *
 * <p>Router implementations are discovered at runtime via
 * {@link java.util.ServiceLoader} using
 * {@link io.kroxylicious.proxy.router.RouterFactory} as the service type.</p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.proxy.router;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
