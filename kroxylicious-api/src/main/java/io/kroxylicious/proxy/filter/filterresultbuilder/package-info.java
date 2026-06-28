/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * <p>Builder API for constructing filter results.</p>
 * <p>Filters use these builders (obtained from {@link io.kroxylicious.proxy.filter.FilterContext})
 * to construct {@link io.kroxylicious.proxy.filter.FilterResult} instances that control
 * message forwarding, dropping, short-circuiting, and connection management.</p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.proxy.filter.filterresultbuilder;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;