/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Context containing old and new configuration state for change detection.
 * This record provides immutable access to configuration data and pre-computed models
 * to avoid excessive logging during change detection.
 */
public record ConfigurationChangeContext(
                                         Configuration oldConfig,
                                         Configuration newConfig,
                                         List<VirtualClusterModel> oldModels,
                                         List<VirtualClusterModel> newModels) {}
