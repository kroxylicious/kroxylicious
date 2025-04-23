/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.function.Consumer;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.MapAssert;

import io.fabric8.kubernetes.api.model.HasMetadata;

@SuppressWarnings("UnusedReturnValue")
public class MetadataAssert<T extends HasMetadata> extends AbstractObjectAssert<MetadataAssert<T>, T> {
    private MetadataAssert(T actual) {
        super(actual, MetadataAssert.class);
    }

    public static <T extends HasMetadata> MetadataAssert<T> assertThat(T actual) {
        return new MetadataAssert<>(actual);
    }

    public MapAssert<String, String> hasAnnotationSatisfying(String annotationName, Consumer<String> expectedValueConsumer) {
        return assertHasObjectMeta().hasAnnotationSatisfying(annotationName, expectedValueConsumer);
    }

    public MapAssert<String, String> assertHasAnnotations() {
        return assertHasObjectMeta().assertAnnotations();
    }

    private ObjectMetaAssert assertHasObjectMeta() {
        assertThat(actual).isNotNull();
        return ObjectMetaAssert.assertThat(actual.getMetadata());
    }
}
