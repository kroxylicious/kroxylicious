/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.function.Consumer;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.ObjectAssert;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;

@SuppressWarnings("UnusedReturnValue")
public class MetadataAssert<T extends HasMetadata> extends AbstractObjectAssert<MetadataAssert<T>, T> {
    private MetadataAssert(T actual) {
        super(actual, MetadataAssert.class);
    }

    public static <T extends HasMetadata> MetadataAssert<T> assertThat(T actual) {
        return new MetadataAssert<>(actual);
    }

    public MapAssert<String, String> hasAnnotationSatisfying(String annotationName, Consumer<String> expectedValueConsumer) {
        return assertHasAnnotations()
                .hasEntrySatisfying(annotationName, expectedValueConsumer);
    }

    public MapAssert<String, String> assertHasAnnotations() {
        return assertHasObjectMeta()
                .extracting(ObjectMeta::getAnnotations)
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class));
    }

    private ObjectAssert<ObjectMeta> assertHasObjectMeta() {
        return assertThat(actual)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(HasMetadata.class))
                .extracting(HasMetadata::getMetadata)
                .isNotNull()
                .asInstanceOf(InstanceOfAssertFactories.type(ObjectMeta.class));
    }
}
