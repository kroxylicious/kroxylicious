/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

/**
 * Marker interface for filter specifications.
 * <p>
 * A {@code FilterSpec} describes which filter the proxy should run and how it should be
 * configured, in terms of the filter's purpose rather than its deployment mechanics.
 * <p>
 * First-party filters ship named implementations (e.g. {@code RecordEncryptionFilterSpec})
 * that expose domain-specific accessors. Fixture implementations know how to translate
 * these concrete types into deployment artifacts (CRDs, config files, etc.).
 * <p>
 * Custom filter authors implement this interface directly for their own filter type.
 * For ad-hoc use without a dedicated class, see {@link RawFilterSpec}.
 */
public interface FilterSpec {
}
