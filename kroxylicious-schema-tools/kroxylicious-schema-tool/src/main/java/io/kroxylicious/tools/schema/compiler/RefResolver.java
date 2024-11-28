/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.net.URI;
import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Pattern;

import io.kroxylicious.tools.schema.model.SchemaObject;

import edu.umd.cs.findbugs.annotations.NonNull;

public class RefResolver extends SchemaObject.Visitor {

    private final Diagnostics diagnostics;
    private final Namer namer;
    private String rootClass;

    public RefResolver(Diagnostics diagnostics,
                       Namer namer,
                       String rootClass) {
        this.diagnostics = diagnostics;
        this.namer = namer;
        this.rootClass = rootClass;
    }

    @Override
    public void enterSchema(
                            URI base,
                            String path,
                            String keyword,
                            @NonNull SchemaObject schema) {
        String ref = schema.getRef();
        if (ref != null) {
            // TODO validate that the other fields are not set

            // resolve
            SchemaObject schemaObject = namer.resolve(base.resolve(ref));
            if (schemaObject == null) {
                // TODO cope with not-yet-loaded refs
                diagnostics.reportWarning("{}: Unable to resolve $ref:{}", base, ref);
            }

            // TODO check for infinite recursion, both direct and indirect
        }
    }

    @Override
    public void exitSchema(
                           URI base,
                           String path,
                           String keyword,
                           @NonNull SchemaObject schema) {
        if (CodeGen.isTypeGenerated(schema) && schema.getJavaType() == null) {
            if (isRootSchema(path)) {
                schema.setJavaType(rootClass);
            }
            else {
                var definitionsMatcher = DEFINITIONS_PATTERN.matcher(path);
                if (definitionsMatcher.matches()) {
                    schema.setJavaType(definitionsMatcher.group("defName"));
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
                        schema.setJavaType(computedName);
                    }
                    else {
                        diagnostics.reportError("Could not compute a java class name for the schema at " + path);
                    }
                }
            }
        }
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

    private static final Pattern PROPS_PATH = Pattern.compile("/(?<keyword>properties|items)/(?<nameOrIndex>[a-zA-Z0-9_-]+)");
    private static final Pattern DEFINITIONS_PATTERN = Pattern.compile(".*/definitions/(?<defName>[a-zA-Z0-9_-]+)");
}
