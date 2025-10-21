package io.kroxylicious.config;

import org.junit.jupiter.api.Test;

import io.kroxylicious.UserNamespace;

import static org.assertj.core.api.Assertions.assertThat;

public class SampleFilterConfigTest {

    private static final String CONFIG_FIND_VALUE = "from";
    private static final String CONFIG_REPLACE_VALUE = "to";

    @Test
    void validateSampleFetchResponseConfigTest() {
        UserNamespace.SampleFilterConfig config = new UserNamespace.SampleFilterConfig(CONFIG_FIND_VALUE, CONFIG_REPLACE_VALUE);
        assertThat(config.getFindValue()).isEqualTo(CONFIG_FIND_VALUE);
        assertThat(config.getReplacementValue()).isEqualTo(CONFIG_REPLACE_VALUE);
    }

    @Test
    void validateSampleFetchResponseConfigEmptyReplacementValueTest() {
        UserNamespace.SampleFilterConfig config = new UserNamespace.SampleFilterConfig(CONFIG_FIND_VALUE, null);
        assertThat(config.getFindValue()).isEqualTo(CONFIG_FIND_VALUE);
        assertThat(config.getReplacementValue()).isEqualTo("");
    }
}
