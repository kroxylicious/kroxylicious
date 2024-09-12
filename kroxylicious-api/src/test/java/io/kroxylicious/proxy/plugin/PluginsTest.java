/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PluginsTest {

    @Test
    void shouldThrowIfRequiredConfigIsAbsent() {
        var e = assertThrows(PluginConfigurationException.class, () -> Plugins.requireConfig(this, null));
        assertEquals("PluginsTest requires configuration, but config object is null", e.getMessage());
    }

}
