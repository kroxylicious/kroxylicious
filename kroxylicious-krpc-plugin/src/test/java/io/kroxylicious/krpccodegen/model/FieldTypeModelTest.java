/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.krpccodegen.schema.FieldType;

import freemarker.ext.beans.GenericObjectModel;
import freemarker.template.SimpleScalar;
import freemarker.template.TemplateBooleanModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class FieldTypeModelTest {
    private static final KrpcSchemaObjectWrapper WRAPPER = new KrpcSchemaObjectWrapper(new Version(2, 3, 34));
    private static final FieldType BYTES_TYPE = FieldType.parse("bytes");
    private static final FieldType STRING_TYPE = FieldType.parse("string");
    private static final FieldType STRUCT_TYPE = FieldType.parse("MyStruct");
    private static final FieldType BOOLEAN_TYPE = FieldType.parse("bool");
    private static final FieldType FLOAT_64_TYPE = FieldType.parse("float64");
    private static final FieldType INT_8_TYPE = FieldType.parse("int8");
    private static final FieldType INT_16_TYPE = FieldType.parse("int16");
    private static final FieldType INT_32_TYPE = FieldType.parse("int32");
    private static final FieldType INT_64_TYPE = FieldType.parse("int64");
    private static final FieldType ARRAY_TYPE = FieldType.parse("[]string");
    private static final FieldType RECORDS_TYPE = FieldType.parse("records");
    private static final FieldType UUID_TYPE = FieldType.parse("uuid");
    private static final FieldType UINT_16_TYPE = FieldType.parse("uint16");
    private static final FieldType STRUCT_ARRAY_TYPE = FieldType.parse("[]MyStruct");

    static Stream<Arguments> isArray() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, false),
                argumentSet("array", ARRAY_TYPE, true),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, true),
                argumentSet("records", RECORDS_TYPE, false),
                argumentSet("string", STRING_TYPE, false),
                argumentSet("struct", STRUCT_TYPE, false),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void isArray(FieldType fieldType, boolean expectedIsArray) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "isArray", expectedIsArray);
    }

    static Stream<Arguments> isStruct() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, false),
                argumentSet("array", ARRAY_TYPE, false),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, false),
                argumentSet("records", RECORDS_TYPE, false),
                argumentSet("string", STRING_TYPE, false),
                argumentSet("struct", STRUCT_TYPE, true),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void isStruct(FieldType fieldType, boolean expectedIsStruct) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "isStruct", expectedIsStruct);
    }

    static Stream<Arguments> isBytes() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, true),
                argumentSet("array", ARRAY_TYPE, false),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, false),
                argumentSet("records", RECORDS_TYPE, false),
                argumentSet("string", STRING_TYPE, false),
                argumentSet("struct", STRUCT_TYPE, false),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void isBytes(FieldType fieldType, boolean expectedIsBytes) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "isBytes", expectedIsBytes);
    }

    static Stream<Arguments> canBeNullable() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, true),
                argumentSet("array", ARRAY_TYPE, true),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, true),
                argumentSet("records", RECORDS_TYPE, true),
                argumentSet("string", STRING_TYPE, true),
                argumentSet("struct", STRUCT_TYPE, true),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void canBeNullable(FieldType fieldType, boolean expectedCanBeNullable) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "canBeNullable", expectedCanBeNullable);
    }

    static Stream<Arguments> fixedLength() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, null),
                argumentSet("array", ARRAY_TYPE, null),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, null),
                argumentSet("records", RECORDS_TYPE, null),
                argumentSet("string", STRING_TYPE, null),
                argumentSet("struct", STRUCT_TYPE, null),
                argumentSet("boolean", BOOLEAN_TYPE, 1),
                argumentSet("float64", FLOAT_64_TYPE, 8),
                argumentSet("int8", INT_8_TYPE, 1),
                argumentSet("int16", INT_16_TYPE, 2),
                argumentSet("int32", INT_32_TYPE, 4),
                argumentSet("int64", INT_64_TYPE, 8),
                argumentSet("uuid", UUID_TYPE, 16),
                argumentSet("uint16", UINT_16_TYPE, 2));
    }

    @MethodSource
    @ParameterizedTest
    void fixedLength(FieldType fieldType, Integer expectedFixedLength) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        TemplateModel fixedLengthModel = fieldTypeModel.get("fixedLength");
        assertThat(fixedLengthModel).isInstanceOfSatisfying(GenericObjectModel.class,
                templateBooleanModel -> assertThat(templateBooleanModel.getWrappedObject()).isEqualTo(Optional.ofNullable(expectedFixedLength)));
    }

    static Stream<Arguments> isStructArray() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, false),
                argumentSet("array", ARRAY_TYPE, false),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, true),
                argumentSet("records", RECORDS_TYPE, false),
                argumentSet("string", STRING_TYPE, false),
                argumentSet("struct", STRUCT_TYPE, false),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void isStructArray(FieldType fieldType, boolean expectedIsStructArray) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "isStructArray", expectedIsStructArray);
    }

    static Stream<Arguments> isFloat() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, false),
                argumentSet("array", ARRAY_TYPE, false),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, false),
                argumentSet("records", RECORDS_TYPE, false),
                argumentSet("string", STRING_TYPE, false),
                argumentSet("struct", STRUCT_TYPE, false),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, true),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void isFloat(FieldType fieldType, boolean expectedIsFloat) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "isFloat", expectedIsFloat);
    }

    static Stream<Arguments> isRecords() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, false),
                argumentSet("array", ARRAY_TYPE, false),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, false),
                argumentSet("records", RECORDS_TYPE, true),
                argumentSet("string", STRING_TYPE, false),
                argumentSet("struct", STRUCT_TYPE, false),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void isRecords(FieldType fieldType, boolean expectedIsRecords) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "isRecords", expectedIsRecords);
    }

    static Stream<Arguments> isVariableLength() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, true),
                argumentSet("array", ARRAY_TYPE, true),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, true),
                argumentSet("records", RECORDS_TYPE, true),
                argumentSet("string", STRING_TYPE, true),
                argumentSet("struct", STRUCT_TYPE, true),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void isVariableLength(FieldType fieldType, boolean expectedIsVariableLength) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "isVariableLength", expectedIsVariableLength);
    }

    static Stream<Arguments> serializationIsDifferentInFlexibleVersions() {
        return Stream.of(argumentSet("bytes", BYTES_TYPE, true),
                argumentSet("array", ARRAY_TYPE, true),
                argumentSet("struct array", STRUCT_ARRAY_TYPE, true),
                argumentSet("records", RECORDS_TYPE, true),
                argumentSet("string", STRING_TYPE, true),
                argumentSet("struct", STRUCT_TYPE, true),
                argumentSet("boolean", BOOLEAN_TYPE, false),
                argumentSet("float64", FLOAT_64_TYPE, false),
                argumentSet("int8", INT_8_TYPE, false),
                argumentSet("int16", INT_16_TYPE, false),
                argumentSet("int32", INT_32_TYPE, false),
                argumentSet("int64", INT_64_TYPE, false),
                argumentSet("uuid", UUID_TYPE, false),
                argumentSet("uint16", UINT_16_TYPE, false));
    }

    @MethodSource
    @ParameterizedTest
    void serializationIsDifferentInFlexibleVersions(FieldType fieldType, boolean expectedIsDifferent) throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, fieldType);
        assertGetPropertyIsBoolean(fieldTypeModel, "serializationIsDifferentInFlexibleVersions", expectedIsDifferent);
    }

    @Test
    void arrayElementType() throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, ARRAY_TYPE);
        TemplateModel elementType = fieldTypeModel.get("elementType");
        assertThat(elementType).isInstanceOfSatisfying(FieldTypeModel.class, fieldTypeModel1 -> assertThat(fieldTypeModel1.fieldType).isEqualTo(STRING_TYPE));
    }

    @Test
    void arrayElementName() throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, ARRAY_TYPE);
        TemplateModel elementType = fieldTypeModel.get("elementName");
        assertThat(elementType).isInstanceOfSatisfying(SimpleScalar.class, fieldTypeModel1 -> assertThat(fieldTypeModel1.getAsString()).isEqualTo("string"));
    }

    @Test
    void structArrayElementName() throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, STRUCT_ARRAY_TYPE);
        TemplateModel elementType = fieldTypeModel.get("elementName");
        assertThat(elementType).isInstanceOfSatisfying(SimpleScalar.class, fieldTypeModel1 -> assertThat(fieldTypeModel1.getAsString()).isEqualTo("MyStruct"));
    }

    @Test
    void structArrayElementType() throws TemplateModelException {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, STRUCT_ARRAY_TYPE);
        TemplateModel elementType = fieldTypeModel.get("elementType");
        assertThat(elementType).isInstanceOfSatisfying(FieldTypeModel.class,
                typeModel -> assertThat(typeModel.fieldType).isInstanceOfSatisfying(FieldType.StructType.class,
                        structType -> assertThat(structType.typeName()).isEqualTo("MyStruct")));
    }

    @Test
    void getUnknownProperty() {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, STRUCT_ARRAY_TYPE);
        assertThatThrownBy(() -> fieldTypeModel.get("unknownProperty")).isInstanceOf(TemplateModelException.class)
                .hasMessage("ArrayType doesn't have property unknownProperty");
    }

    @Test
    void isEmpty() {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, STRUCT_ARRAY_TYPE);
        assertThat(fieldTypeModel.isEmpty()).isFalse();
    }

    @Test
    void getAdaptedObject() {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, STRUCT_ARRAY_TYPE);
        assertThat(fieldTypeModel.getAdaptedObject(FieldType.class)).isEqualTo(STRUCT_ARRAY_TYPE);
    }

    @Test
    void getAsString() {
        FieldTypeModel fieldTypeModel = new FieldTypeModel(WRAPPER, STRUCT_ARRAY_TYPE);
        assertThat(fieldTypeModel.getAsString()).isEqualTo("[]MyStruct");
    }

    private static void assertGetPropertyIsBoolean(FieldTypeModel fieldTypeModel, String field, boolean expectedIsRecords) throws TemplateModelException {
        TemplateModel templateModel = fieldTypeModel.get(field);
        assertThat(templateModel).isInstanceOfSatisfying(TemplateBooleanModel.class, templateBooleanModel -> {
            try {
                assertThat(templateBooleanModel.getAsBoolean()).isEqualTo(expectedIsRecords);
            }
            catch (TemplateModelException e) {
                throw new RuntimeException(e);
            }
        });
    }

}