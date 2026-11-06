/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.krpccodegen.schema.ApiSpec;
import io.kroxylicious.krpccodegen.schema.FieldSpec;
import io.kroxylicious.krpccodegen.schema.FieldType;
import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.StructSpec;
import io.kroxylicious.krpccodegen.schema.Versions;

import freemarker.template.ObjectWrapperAndUnwrapper;
import freemarker.template.SimpleScalar;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KrpcSchemaObjectWrapperTest {

    private static final KrpcSchemaObjectWrapper WRAPPER = new KrpcSchemaObjectWrapper(new Version(2, 3, 34));

    private static final MessageSpec MESSAGE_SPEC = mock(MessageSpec.class);
    private static final ApiSpec API_SPEC = mock(ApiSpec.class);
    private static final FieldSpec FIELD_SPEC = mock(FieldSpec.class);
    private static final FieldType FIELD_TYPE = mock(FieldType.class);
    private static final StructSpec STRUCT_SPEC = mock(StructSpec.class);
    private static final Versions VERSIONS = new Versions((short) 4, (short) 5);

    static Stream<Arguments> schemaObjects() {
        return Stream.of(
                Arguments.argumentSet("MessageSpec", MESSAGE_SPEC, MessageSpecModel.class),
                Arguments.argumentSet("ApiSpec", API_SPEC, ApiSpecModel.class),
                Arguments.argumentSet("FieldSpec", FIELD_SPEC, FieldSpecModel.class),
                Arguments.argumentSet("FieldType", FIELD_TYPE, FieldTypeModel.class),
                Arguments.argumentSet("StructSpec", STRUCT_SPEC, StructSpecModel.class),
                Arguments.argumentSet("Versions", VERSIONS, VersionsModel.class));
    }

    @ParameterizedTest
    @MethodSource("schemaObjects")
    void wrapShouldCreateModelForSchemaObject(Object schemaObject, Class<? extends TemplateModel> expectedModelType) throws TemplateModelException {
        // When
        TemplateModel model = WRAPPER.wrap(schemaObject);

        // Then
        assertThat(model).isInstanceOf(expectedModelType);
    }

    static Stream<Arguments> unwrappableModels() {
        return Stream.of(
                Arguments.argumentSet("MessageSpecModel", (Function<KrpcSchemaObjectWrapper, TemplateModel>) w -> new MessageSpecModel(w, MESSAGE_SPEC), MESSAGE_SPEC,
                        MessageSpec.class),
                Arguments.argumentSet("FieldSpecModel", (Function<KrpcSchemaObjectWrapper, TemplateModel>) w -> new FieldSpecModel(w, FIELD_SPEC), FIELD_SPEC,
                        FieldSpec.class),
                Arguments.argumentSet("StructSpecModel", (Function<KrpcSchemaObjectWrapper, TemplateModel>) w -> new StructSpecModel(w, STRUCT_SPEC), STRUCT_SPEC,
                        StructSpec.class),
                Arguments.argumentSet("FieldTypeModel", (Function<KrpcSchemaObjectWrapper, TemplateModel>) w -> new FieldTypeModel(w, FIELD_TYPE), FIELD_TYPE,
                        FieldType.class),
                Arguments.argumentSet("VersionsModel", (Function<KrpcSchemaObjectWrapper, TemplateModel>) w -> new VersionsModel(w, VERSIONS), VERSIONS,
                        Versions.class));
    }

    static Stream<Arguments> allModels() {
        return Stream.concat(unwrappableModels(),
                Stream.of(Arguments.argumentSet("ApiSpecModel", (Function<KrpcSchemaObjectWrapper, TemplateModel>) w -> new ApiSpecModel(w, API_SPEC), API_SPEC,
                        ApiSpec.class)));
    }

    @ParameterizedTest
    @MethodSource("allModels")
    void unwrapShouldReturnSchemaObject(Function<KrpcSchemaObjectWrapper, TemplateModel> modelFactory, Object schemaObject, Class<?> ignoredTargetClass)
            throws TemplateModelException {
        // Given
        TemplateModel model = modelFactory.apply(WRAPPER);

        // When
        Object unwrapped = WRAPPER.unwrap(model);

        // Then
        assertThat(unwrapped).isSameAs(schemaObject);
    }

    @ParameterizedTest
    @MethodSource("unwrappableModels")
    void tryUnwrapToShouldReturnSchemaObjectWhenTargetClassMatches(Function<KrpcSchemaObjectWrapper, TemplateModel> modelFactory, Object schemaObject,
                                                                   Class<?> targetClass)
            throws TemplateModelException {
        // Given
        TemplateModel model = modelFactory.apply(WRAPPER);

        // When
        Object unwrapped = WRAPPER.tryUnwrapTo(model, targetClass);

        // Then
        assertThat(unwrapped).isSameAs(schemaObject);
    }

    @ParameterizedTest
    @MethodSource("unwrappableModels")
    void tryUnwrapToShouldDelegateWhenTargetClassDoesNotMatch(Function<KrpcSchemaObjectWrapper, TemplateModel> modelFactory, Object ignored, Class<?> ignoredTargetClass)
            throws TemplateModelException {
        // Given
        TemplateModel model = modelFactory.apply(WRAPPER);

        // When
        Object unwrapped = WRAPPER.tryUnwrapTo(model, Integer.class);

        // Then
        assertThat(unwrapped).isSameAs(ObjectWrapperAndUnwrapper.CANT_UNWRAP_TO_TARGET_CLASS);
    }

    @Test
    void wrapShouldReturnTemplateModelUnchanged() throws TemplateModelException {
        // Given
        TemplateModel model = new SimpleScalar("foo");

        // When
        TemplateModel wrapped = WRAPPER.wrap(model);

        // Then
        assertThat(wrapped).isSameAs(model);
    }

    @Test
    void wrapShouldDelegateForOtherObjects() throws TemplateModelException {
        // When
        TemplateModel wrapped = WRAPPER.wrap("foo");

        // Then
        assertThat(wrapped).isInstanceOfSatisfying(SimpleScalar.class, scalar -> assertThat(scalar.getAsString()).isEqualTo("foo"));
    }

    @Test
    void wrapShouldDelegateForNull() throws TemplateModelException {
        // When
        TemplateModel wrapped = WRAPPER.wrap(null);

        // Then
        assertThat(wrapped).isNull();
    }

    @Test
    void unwrapShouldDelegateForOtherModels() throws TemplateModelException {
        // When
        Object unwrapped = WRAPPER.unwrap(new SimpleScalar("foo"));

        // Then
        assertThat(unwrapped).isEqualTo("foo");
    }

    @Test
    void tryUnwrapToShouldDelegateForOtherModels() throws TemplateModelException {
        // When
        Object unwrapped = WRAPPER.tryUnwrapTo(new SimpleScalar("foo"), String.class);

        // Then
        assertThat(unwrapped).isEqualTo("foo");
    }
}
