/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import org.junit.jupiter.api.Test;

import io.kroxylicious.krpccodegen.schema.Versions;

import freemarker.template.SimpleNumber;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.Version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VersionsModelTest {
    private static final KrpcSchemaObjectWrapper WRAPPER = new KrpcSchemaObjectWrapper(new Version(2, 3, 34));
    public static final Versions VERSIONS = new Versions((short) 4, (short) 5);

    @Test
    void highest() throws TemplateModelException {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        TemplateModel highest = versionsModel.get("highest");
        assertThat(highest).isInstanceOfSatisfying(SimpleNumber.class, simpleNumber -> assertThat(simpleNumber.getAsNumber()).isEqualTo((short) 5));
    }

    @Test
    void lowest() throws TemplateModelException {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        TemplateModel lowest = versionsModel.get("lowest");
        assertThat(lowest).isInstanceOfSatisfying(SimpleNumber.class, simpleNumber -> assertThat(simpleNumber.getAsNumber()).isEqualTo((short) 4));
    }

    @Test
    void intersect() throws TemplateModelException {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        TemplateModel lambda = versionsModel.get("intersect");
        assertThat(lambda).isNotNull();
    }

    @Test
    void contains() throws TemplateModelException {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        TemplateModel lambda = versionsModel.get("contains");
        assertThat(lambda).isNotNull();
    }

    @Test
    void unknownProperty() {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        assertThatThrownBy(() -> versionsModel.get("unknown")).isInstanceOf(TemplateModelException.class).hasMessage("Versions doesn't have property unknown");
    }

    @Test
    void isEmpty() {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        assertThat(versionsModel.isEmpty()).isFalse();
    }

    @Test
    void getAdaptedObject() {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        assertThat(versionsModel.getAdaptedObject(Versions.class)).isEqualTo(VERSIONS);
    }

    @Test
    void getAsString() {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        assertThat(versionsModel.getAsString()).isEqualTo("4-5");
    }

    @Test
    void get() throws TemplateModelException {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        int index = 3;
        TemplateModel model = versionsModel.get(index);
        assertThat(model).isInstanceOfSatisfying(SimpleNumber.class, simpleNumber -> assertThat(simpleNumber.getAsNumber()).isEqualTo((short) 4 + index));
    }

    @Test
    void size() {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, VERSIONS);
        int size = versionsModel.size();
        assertThat(size).isEqualTo(2);
    }

    @Test
    void sizeNone() {
        VersionsModel versionsModel = new VersionsModel(WRAPPER, Versions.NONE);
        int size = versionsModel.size();
        assertThat(size).isZero();
    }

}