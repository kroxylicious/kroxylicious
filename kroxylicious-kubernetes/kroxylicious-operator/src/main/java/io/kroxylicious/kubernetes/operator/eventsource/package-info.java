/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * A declarative DSL ({@link io.kroxylicious.kubernetes.operator.eventsource.Ref}) and fluent builder
 * ({@link io.kroxylicious.kubernetes.operator.eventsource.EventSourceBuilder}) for registering JOSDK event
 * sources, covering both relationship directions a reconciler may need: a primary holding a reference to its
 * secondaries, or a secondary (possibly transitively) holding a reference back to its primary.
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kubernetes.operator.eventsource;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
