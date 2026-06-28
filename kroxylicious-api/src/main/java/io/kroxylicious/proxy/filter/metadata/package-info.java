/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * <p>APIs for resolving topic IDs to topic names and handling metadata-related errors.</p>
 * <p>Filters that need to translate topic IDs (e.g., from Fetch requests) to topic names
 * can use {@link io.kroxylicious.proxy.filter.metadata.TopicNameMapping} to represent
 * the results of such resolution, including handling of partial failures.</p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.proxy.filter.metadata;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
