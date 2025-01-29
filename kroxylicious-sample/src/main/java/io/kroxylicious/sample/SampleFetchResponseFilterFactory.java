/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.sample.config.SampleFilterConfig;

/**
 * A {@link FilterFactory} for {@link SampleFetchResponseFilter}.
 *
 * @deprecated use {@link SampleFetchResponse} instead.
 */
@Plugin(configType = SampleFilterConfig.class)
@Deprecated(since = "0.10.0", forRemoval = true)
public class SampleFetchResponseFilterFactory extends SampleFetchResponse {

}
