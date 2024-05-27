/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import edu.umd.cs.findbugs.annotations.Nullable;

public record ResourceMetadata(
                               @Nullable ConfigResourceMetadataSource configSource) {
    /* eventually we'll impose a oneOf constraint on multiple different sources of metadata */

    @Override
    @Nullable
    public ConfigResourceMetadataSource configSource() {
        return configSource;
    }
}
