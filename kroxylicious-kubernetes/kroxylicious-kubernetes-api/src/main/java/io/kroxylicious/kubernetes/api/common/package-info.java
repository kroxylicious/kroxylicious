/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Types shared between the Kroxylicious custom resource APIs, such as
 * {@linkplain io.kroxylicious.kubernetes.api.common.LocalRef references between resources},
 * {@linkplain io.kroxylicious.kubernetes.api.common.Condition status conditions} and
 * TLS-related types. The CRD-to-Java code generator is configured to map structurally
 * identical types occurring in several CRD schemas onto the hand-written classes in
 * this package, so that they share a single Java type.
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.kubernetes.api.common;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;