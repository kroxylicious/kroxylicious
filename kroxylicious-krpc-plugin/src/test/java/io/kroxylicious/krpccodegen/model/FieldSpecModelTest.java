/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.krpccodegen.schema.EntityType;
import io.kroxylicious.krpccodegen.schema.FieldSpec;
import io.kroxylicious.krpccodegen.schema.FieldType;
import io.kroxylicious.krpccodegen.schema.Versions;

import freemarker.ext.beans.GenericObjectModel;
import freemarker.template.DefaultListAdapter;
import freemarker.template.SimpleNumber;
import freemarker.template.SimpleScalar;
import freemarker.template.TemplateBooleanModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FieldSpecModelTest {

    private static final String ABOUT = "about";
    private static final EntityType ENTITY_TYPE = EntityType.UNKNOWN;
    private static final String FIELD_DEFAULT = "true";
    private static final boolean IGNORABLE = false;
    private static final boolean ZERO_COPY = false;
    private static final int TAG = 1;
    private static final FieldSpec BOOL_FIELD_SPEC = new FieldSpec("boolField", "4+", List.of(), "bool", false, null, FIELD_DEFAULT, IGNORABLE, ENTITY_TYPE, ABOUT, "4+",
            null, TAG, ZERO_COPY);
    private static final KrpcSchemaObjectWrapper WRAPPER = new KrpcSchemaObjectWrapper(new Version(2, 3, 34));

    @Test
    void getFieldSpec() throws TemplateModelException {
        TemplateModel name = whenGetTemplateModel("name");
        assertThat(name).isInstanceOfSatisfying(SimpleScalar.class, simpleScalar -> assertThat(simpleScalar.getAsString()).isEqualTo("boolField"));
    }

    @Test
    void getType() throws TemplateModelException {
        TemplateModel typeModel = whenGetTemplateModel("type");
        assertThat(typeModel).isInstanceOfSatisfying(FieldTypeModel.class,
                fieldTypeModel -> assertThat(fieldTypeModel.fieldType).isInstanceOf(FieldType.BoolFieldType.class));
    }

    @Test
    void getTypeString() throws TemplateModelException {
        TemplateModel typeStringModel = whenGetTemplateModel("typeString");
        assertThat(typeStringModel).isInstanceOfSatisfying(SimpleScalar.class, fieldTypeModel -> assertThat(fieldTypeModel.getAsString()).isEqualTo("bool"));
    }

    @Test
    void getAbout() throws TemplateModelException {
        TemplateModel aboutModel = whenGetTemplateModel("about");
        assertThat(aboutModel).isInstanceOfSatisfying(SimpleScalar.class, fieldTypeModel -> assertThat(fieldTypeModel.getAsString()).isEqualTo(ABOUT));
    }

    @Test
    void getEntityType() throws TemplateModelException {
        TemplateModel entityType = whenGetTemplateModel("entityType");
        assertThat(entityType).isInstanceOfSatisfying(GenericObjectModel.class,
                genericObjectModel -> assertThat(genericObjectModel.getWrappedObject()).isEqualTo(ENTITY_TYPE));
    }

    @Test
    void getFlexibleVersions() throws TemplateModelException {
        TemplateModel flexibleVersions = whenGetTemplateModel("flexibleVersions");
        assertThat(flexibleVersions).isInstanceOfSatisfying(GenericObjectModel.class,
                genericObjectModel -> assertThat(genericObjectModel.getWrappedObject()).isEqualTo(Optional.empty()));
    }

    @Test
    void getFlexibleVersionsString() throws TemplateModelException {
        TemplateModel flexibleVersionsString = whenGetTemplateModel("flexibleVersionsString");
        assertThat(flexibleVersionsString).isNull();
    }

    @Test
    void getDefaultString() throws TemplateModelException {
        TemplateModel defaultModel = whenGetTemplateModel("defaultString");
        assertThat(defaultModel).isInstanceOfSatisfying(SimpleScalar.class, fieldTypeModel -> assertThat(fieldTypeModel.getAsString()).isEqualTo(FIELD_DEFAULT));
    }

    @Test
    void getIgnorable() throws TemplateModelException {
        TemplateModel ignorable = whenGetTemplateModel("ignorable");
        assertThat(ignorable).isInstanceOfSatisfying(TemplateBooleanModel.class, fieldTypeModel -> {
            try {
                assertThat(fieldTypeModel.getAsBoolean()).isEqualTo(IGNORABLE);
            }
            catch (TemplateModelException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void getNullableVersions() throws TemplateModelException {
        TemplateModel versions = whenGetTemplateModel("nullableVersions");
        assertThat(versions).isInstanceOfSatisfying(VersionsModel.class, versionsModel -> assertThat(versionsModel.versions).isEqualTo(Versions.NONE));
    }

    @Test
    void getNullableVersionsString() throws TemplateModelException {
        TemplateModel versions = whenGetTemplateModel("nullableVersionsString");
        assertThat(versions).isInstanceOfSatisfying(SimpleScalar.class, versionsModel -> assertThat(versionsModel.getAsString()).isEqualTo("none"));
    }

    @Test
    void getTag() throws TemplateModelException {
        TemplateModel tag = whenGetTemplateModel("tag");
        assertThat(tag).isInstanceOfSatisfying(GenericObjectModel.class,
                genericObjectModel -> assertThat(genericObjectModel.getWrappedObject()).isEqualTo(Optional.of(TAG)));
    }

    @Test
    void getTaggedVersions() throws TemplateModelException {
        TemplateModel tag = whenGetTemplateModel("taggedVersions");
        assertThat(tag).isInstanceOfSatisfying(VersionsModel.class,
                versionsModel -> assertThat(versionsModel.versions).isEqualTo(new Versions((short) 4, Short.MAX_VALUE)));
    }

    @Test
    void getTaggedVersionsAsString() throws TemplateModelException {
        TemplateModel tag = whenGetTemplateModel("taggedVersionsString");
        assertThat(tag).isInstanceOfSatisfying(SimpleScalar.class, versionsModel -> assertThat(versionsModel.getAsString()).isEqualTo("4+"));
    }

    @Test
    void getTagInteger() throws TemplateModelException {
        TemplateModel tag = whenGetTemplateModel("tagInteger");
        assertThat(tag).isInstanceOfSatisfying(SimpleNumber.class, tagModel -> assertThat(tagModel.getAsNumber()).isEqualTo(1));
    }

    @Test
    void getVersions() throws TemplateModelException {
        TemplateModel versions = whenGetTemplateModel("versions");
        assertThat(versions).isInstanceOfSatisfying(VersionsModel.class,
                versionsModel -> assertThat(versionsModel.versions).isEqualTo(new Versions((short) 4, Short.MAX_VALUE)));
    }

    @Test
    void getVersionsString() throws TemplateModelException {
        TemplateModel versionsString = whenGetTemplateModel("versionsString");
        assertThat(versionsString).isInstanceOfSatisfying(SimpleScalar.class, versionsModel -> assertThat(versionsModel.getAsString()).isEqualTo("4+"));
    }

    @Test
    void getMapKey() throws TemplateModelException {
        TemplateModel mapKey = whenGetTemplateModel("mapKey");
        assertThat(mapKey).isInstanceOfSatisfying(TemplateBooleanModel.class, mapKeyModel -> {
            try {
                assertThat(mapKeyModel.getAsBoolean()).isFalse();
            }
            catch (TemplateModelException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void getZeroCopy() throws TemplateModelException {
        String key = "zeroCopy";
        TemplateModel tag = whenGetTemplateModel(key);
        assertThat(tag).isInstanceOfSatisfying(TemplateBooleanModel.class, zeroCopyModel -> {
            try {
                assertThat(zeroCopyModel.getAsBoolean()).isEqualTo(ZERO_COPY);
            }
            catch (TemplateModelException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void getFieldsSpec() throws TemplateModelException {
        FieldSpec outerField = new FieldSpec("StructType", "4+", List.of(BOOL_FIELD_SPEC), "MyStruct", false, null, "default", false, ENTITY_TYPE, ABOUT, "4+",
                null, TAG, false);
        FieldSpecModel model = new FieldSpecModel(WRAPPER, outerField);
        TemplateModel name = model.get("fields");
        assertThat(name).isInstanceOfSatisfying(DefaultListAdapter.class, listAdapter -> {
            try {
                assertThat(listAdapter.size()).isEqualTo(TAG);
                assertThat(listAdapter.get(0)).isInstanceOfSatisfying(FieldSpecModel.class, fieldSpecModel -> assertThat(fieldSpecModel.spec).isSameAs(BOOL_FIELD_SPEC));
            }
            catch (TemplateModelException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void getUnknownKey() {
        FieldSpecModel model = new FieldSpecModel(WRAPPER, BOOL_FIELD_SPEC);
        assertThatThrownBy(() -> model.get("blart-not-found"))
                .isInstanceOf(TemplateModelException.class).hasMessage("FieldSpec doesn't have property blart-not-found");
    }

    @Test
    void isEmpty() {
        FieldSpecModel model = new FieldSpecModel(WRAPPER, BOOL_FIELD_SPEC);
        assertThat(model.isEmpty()).isFalse();
    }

    @Test
    void getAdaptedObject() {
        FieldSpecModel model = new FieldSpecModel(WRAPPER, BOOL_FIELD_SPEC);
        assertThat(model.getAdaptedObject(FieldSpec.class)).isEqualTo(BOOL_FIELD_SPEC);
    }

    private static TemplateModel whenGetTemplateModel(String key) throws TemplateModelException {
        FieldSpecModel model = new FieldSpecModel(WRAPPER, BOOL_FIELD_SPEC);
        return model.get(key);
    }
}