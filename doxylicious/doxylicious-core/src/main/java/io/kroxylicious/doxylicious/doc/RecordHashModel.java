/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.doc;

import java.lang.reflect.Method;
import java.util.Map;

import javax.annotation.Nullable;

import freemarker.template.TemplateHashModel;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

@SuppressWarnings("java:S6213") // sonar is stupid for being so opinionated about using record as an identifier
class RecordHashModel implements TemplateHashModel {

    private final RecordWrapper recordWrapper;
    private final Map<String, Method> handles;
    private final Record record;

    RecordHashModel(
                    RecordWrapper recordWrapper,
                    Map<String, Method> handles,
                    Record record) {
        this.recordWrapper = recordWrapper;
        this.handles = handles;
        this.record = record;
    }

    Record record() {
        return record;
    }

    @Override
    public @Nullable TemplateModel get(String key) throws TemplateModelException {
        Method method = handles.get(key);
        if (method != null) {
            try {
                return recordWrapper.wrap(method.invoke(record));
            }
            catch (ReflectiveOperationException e) {
                throw new TemplateModelException(e);
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() throws TemplateModelException {
        return handles.isEmpty();
    }
}
