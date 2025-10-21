/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import freemarker.ext.beans.GenericObjectModel;
import freemarker.template.ObjectWrapper;
import freemarker.template.ObjectWrapperAndUnwrapper;
import freemarker.template.SimpleSequence;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;

final class ModelUtils {

    private ModelUtils() {
        // static utility class
    }

    @SuppressWarnings("rawtypes") // Freemarker uses raw lists for its arguments
    static SimpleSequence modelArgsToSimpleSequence(List args, ObjectWrapper wrapper) throws TemplateModelException {
        var seq = new SimpleSequence(wrapper);
        for (Object objOrSeq : args) {
            if (objOrSeq instanceof SimpleSequence ss) {
                for (int i = 0; i < ss.size(); i++) {
                    var obj = ss.get(i);
                    seq.add(obj);
                }
            }
            else if (objOrSeq instanceof GenericObjectModel gom) {
                seq.add(gom);
            }
            else {
                throw new TemplateModelException("Unsupported argument type " + objOrSeq.getClass().getName() + " found in arguments.");
            }
        }
        return seq;
    }

    static <E extends Enum<E>> Set<E> asEnumSet(SimpleSequence seq, Class<E> enumClazz) throws TemplateModelException {
        var ow = (ObjectWrapperAndUnwrapper) seq.getObjectWrapper();
        var set = new HashSet<E>(seq.size());
        for (int i = 0; i < seq.size(); i++) {
            try {
                TemplateModel obj = seq.get(i);
                var unwrapped = ow.unwrap(obj);
                set.add(enumClazz.isInstance(unwrapped) ? enumClazz.cast(unwrapped) : Enum.valueOf(enumClazz, String.valueOf(unwrapped)));
            }
            catch (TemplateModelException e) {
                throw new TemplateModelException("Failed to unwrap template model object at index " + i, e);
            }
        }
        return set;
    }
}
