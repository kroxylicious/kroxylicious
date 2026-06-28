/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.krpccodegen.schema.StructSpec;

import freemarker.template.SimpleScalar;
import freemarker.template.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StructSpecModelTest {

    private static final KrpcSchemaObjectWrapper WRAPPER = new KrpcSchemaObjectWrapper(new Version(2, 3, 34));

    @Mock
    private StructSpec structSpec;

    @Test
    void name() throws Exception {
        // Given
        when(structSpec.name()).thenReturn("foo");
        var model = new StructSpecModel(WRAPPER, structSpec);

        // WHen
        var result = model.get("name");
        assertThat(result)
                .asInstanceOf(InstanceOfAssertFactories.type(SimpleScalar.class))
                .extracting(SimpleScalar::getAsString)
                .isEqualTo("foo");
    }
}