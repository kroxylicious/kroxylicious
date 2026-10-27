/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * A path-composition DSL ({@link ReferenceDependency} and {@link StaticDependency}) and fluent builder
 * ({@link io.kroxylicious.kubernetes.operator.eventsource.EventSourceBuilder}) for registering JOSDK event
 * sources, covering any reference chain a reconciler may need between its primary and a secondary, of any
 * length and in either direction, regardless of which end of the chain holds which reference.
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kubernetes.operator.eventsource;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
