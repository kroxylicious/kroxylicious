/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation;

import io.kroxylicious.proxy.filter.validation.config.ValidationConfig;
import io.kroxylicious.proxy.plugin.Plugin;

/**
 * A {@link io.kroxylicious.proxy.filter.FilterFactory} for {@link RecordValidationFilter}.
 * @deprecated Replaced with {@link RecordValidation}
 */
@Plugin(configType = ValidationConfig.class)
@Deprecated(since = "0.7.0", forRemoval = true)
public class ProduceValidationFilterFactory extends RecordValidation {
}
