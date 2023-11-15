/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SampleFilterConfigTest {

    private static final String CONFIG_REPLACER_MODULE = "foobar.wasm";

    @Test
    void validateSampleFetchResponseConfigTest() {
        SampleFilterConfig config = new SampleFilterConfig(CONFIG_REPLACER_MODULE);
        assertThat(config.getReplacerModule()).isEqualTo(CONFIG_REPLACER_MODULE);
    }

}
