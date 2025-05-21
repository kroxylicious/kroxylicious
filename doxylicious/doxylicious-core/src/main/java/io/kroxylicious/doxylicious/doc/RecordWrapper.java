/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.doc;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.HashMap;
import java.util.Map;

import freemarker.template.SimpleObjectWrapper;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

@SuppressWarnings("java:S6213") // sonar is stupid for being so opinionated about using record as an identifier
class RecordWrapper extends SimpleObjectWrapper {
    Map<Class<? extends Record>, Map<String, Method>> methodsByRecord = new HashMap<>();

    @Override
    public TemplateModel wrap(Object obj) throws TemplateModelException {
        if (obj instanceof Record record) {
            var accessorMap = methodsByRecord.computeIfAbsent(record.getClass(), recordClass -> {
                RecordComponent[] recordComponents = recordClass.getRecordComponents();
                Map<String, Method> accessors = new HashMap<>(1 + (int) (recordComponents.length / 0.75));
                for (var c : recordComponents) {
                    Method accessor = c.getAccessor();
                    accessors.put(c.getName(), accessor);
                }
                return accessors;
            });
            return new RecordHashModel(this, accessorMap, record);
        }
        return super.wrap(obj);
    }

    @Override
    public Object unwrap(TemplateModel tm) throws TemplateModelException {
        if (tm instanceof RecordHashModel rhm) {
            return rhm.record();
        }
        return super.unwrap(tm);
    }
}
