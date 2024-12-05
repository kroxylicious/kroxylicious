/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Pattern;

import io.kroxylicious.tools.schema.model.SchemaObject;
import io.kroxylicious.tools.schema.model.SchemaVisitor;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link SchemaVisitor} that assigns a (hopefully, but not necessarily)
 * unique {@link SchemaObject#getJavaType()} name to each (sub-)schema which has
 * been loaded without any explicit {@link SchemaObject#getJavaType()}.
 *
 * We don't aim for uniqueness on the basis that the user can always override by setting $javaType, and we'd prefer to
 * generate "nice" names than "ugly" names which are guaranteed to be unique.
 */
public class TypeNameVisitor extends SchemaVisitor {

    private final Diagnostics diagnostics;
    private final IdVisitor idVisitor;
    private String rootClass;

    public TypeNameVisitor(Diagnostics diagnostics,
                           IdVisitor idVisitor,
                           String rootClass) {
        this.diagnostics = diagnostics;
        this.idVisitor = idVisitor;
        this.rootClass = rootClass;
    }

    @Override
    public void enterSchema(
                            SchemaVisitor.Context context,
                            @NonNull SchemaObject schema) {
        String ref = schema.getRef();
        if (ref != null) {
            // TODO validate that the other fields are not set

            // resolve
            SchemaObject schemaObject = idVisitor.resolve(context.base().resolve(ref));
            if (schemaObject == null) {
                // TODO cope with not-yet-loaded refs
                diagnostics.reportWarning("{}: Unable to resolve $ref:{}", context.base(), ref);
            }

            // TODO check for infinite recursion, both direct and indirect
        }
    }

    @Override
    public void exitSchema(
                           SchemaVisitor.Context context,
                           @NonNull SchemaObject schema) {
        if (CodeGen.isTypeGenerated(schema) && schema.getJavaType() == null) {
            if (context.isRootSchema()) {
                schema.setJavaType(rootClass);
            }
            else {
                final String name = generateTypeName(context.fullPath(), context.keyword());
                schema.setJavaType(name);
            }
        }
    }

    private String generateTypeName(String path, String keyword) {
        final String name;
        if ("definitions".equals(keyword)) {
            name = generateTypeNameForDefinition(path);
        }
        else {
            var propsMatcher = PROPS_PATH.matcher(path);
            var nameParts = new ArrayList<String>();
            var singularize = new ArrayList<Integer>();
            nameParts.add(rootClass);
            while (propsMatcher.find()) {
                String keyword2 = propsMatcher.group("keyword");
                if ("items".equals(keyword2)) {
                    singularize.add(nameParts.size() - 1);
                    continue;
                }
                String propertyName = propsMatcher.group("nameOrIndex");
                propertyName = initialCaps(propertyName);
                nameParts.add(propertyName);
            }
            for (int index : singularize) {
                nameParts.add(index, singularize(nameParts.remove(index)));
            }
            String computedName = String.join("", nameParts);
            assert (!computedName.isEmpty());
            if (!computedName.equals(rootClass)) {
                name = computedName;
            }
            else {
                diagnostics.reportError("Could not compute a java class name for the schema at " + path);
                name = "*ERROR*";
            }
        }
        return name;
    }

    private String generateTypeNameForDefinition(String path) {
        final String name;
        var definitionsMatcher = DEFINITIONS_PATTERN.matcher(path);
        if (definitionsMatcher.matches()) {
            name = definitionsMatcher.group("defName");
        }
        else {
            diagnostics.reportError("Could not compute a java class name for the schema at " + path);
            name = "*ERROR*";
        }
        return name;
    }

    @NonNull
    private static String initialCaps(String propertyName) {
        return propertyName.substring(0, 1).toUpperCase(Locale.ROOT) + propertyName.substring(1);
    }

    @NonNull
    private static String singularize(String propertyName) {
        if (propertyName.endsWith("ies")) {
            propertyName = propertyName.substring(0, propertyName.length() - 3);
        }
        else if (propertyName.endsWith("es")) {
            propertyName = propertyName.substring(0, propertyName.length() - 2);
        }
        else if (propertyName.endsWith("s")) {
            propertyName = propertyName.substring(0, propertyName.length() - 1);
        }
        return propertyName;
    }

    @SuppressWarnings("java:S5860") // sonar fails to detect use of the group name
    private static final Pattern PROPS_PATH = Pattern.compile("/(?<keyword>properties|items)/(?<nameOrIndex>[a-zA-Z0-9_-]+)");
    @SuppressWarnings("java:S5860") // sonar fails to detect use of the group name
    private static final Pattern DEFINITIONS_PATTERN = Pattern.compile(".*/definitions/(?<defName>[a-zA-Z0-9_-]+)");
}
