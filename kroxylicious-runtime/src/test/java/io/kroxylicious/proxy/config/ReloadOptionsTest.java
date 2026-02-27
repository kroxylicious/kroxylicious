/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import static org.assertj.core.api.Assertions.assertThat;

class ReloadOptionsTest {

    @Test
    void defaultConstantShouldRollbackAndPersist() {
        assertThat(ReloadOptions.DEFAULT.onFailure()).isEqualTo(OnFailure.ROLLBACK);
        assertThat(ReloadOptions.DEFAULT.persistConfigToDisk()).isTrue();
    }

    @Test
    void effectiveOnFailureShouldReturnValueWhenSpecified() {
        ReloadOptions options = new ReloadOptions(OnFailure.TERMINATE, false);
        assertThat(options.effectiveOnFailure()).isEqualTo(OnFailure.TERMINATE);
    }

    @Test
    void effectiveOnFailureShouldDefaultToRollbackWhenNull() {
        ReloadOptions options = new ReloadOptions(null, false);
        assertThat(options.effectiveOnFailure()).isEqualTo(OnFailure.ROLLBACK);
    }

    @Test
    void shouldDeserializeFromYaml() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        String yaml = """
                onFailure: TERMINATE
                persistConfigToDisk: false
                """;
        ReloadOptions options = mapper.readValue(yaml, ReloadOptions.class);
        assertThat(options.onFailure()).isEqualTo(OnFailure.TERMINATE);
        assertThat(options.persistConfigToDisk()).isFalse();
    }

    @Test
    void shouldDeserializePartialYaml() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        String yaml = """
                persistConfigToDisk: false
                """;
        ReloadOptions options = mapper.readValue(yaml, ReloadOptions.class);
        assertThat(options.onFailure()).isNull();
        assertThat(options.effectiveOnFailure()).isEqualTo(OnFailure.ROLLBACK);
        assertThat(options.persistConfigToDisk()).isFalse();
    }

    @Test
    void shouldDeserializeAllOnFailureValues() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        for (OnFailure value : OnFailure.values()) {
            String yaml = "onFailure: %s\npersistConfigToDisk: true\n".formatted(value.name());
            ReloadOptions options = mapper.readValue(yaml, ReloadOptions.class);
            assertThat(options.onFailure()).isEqualTo(value);
        }
    }
}
