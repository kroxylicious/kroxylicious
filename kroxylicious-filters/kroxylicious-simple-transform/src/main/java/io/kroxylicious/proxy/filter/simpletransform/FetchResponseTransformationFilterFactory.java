/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.simpletransform.FetchResponseTransformation.Config;
import io.kroxylicious.proxy.plugin.Plugin;

/**
 * A {@link FilterFactory} for {@link FetchResponseTransformationFilter}.
 *
 * @deprecated use {@link FetchResponseTransformation} instead.
 */
@Plugin(configType = Config.class)
@Deprecated(since = "0.10.0", forRemoval = true)
public class FetchResponseTransformationFilterFactory extends FetchResponseTransformation {
}
