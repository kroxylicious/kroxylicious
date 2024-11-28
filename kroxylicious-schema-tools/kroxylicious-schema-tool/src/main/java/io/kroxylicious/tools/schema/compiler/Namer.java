/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.tools.schema.model.SchemaObject;

import edu.umd.cs.findbugs.annotations.NonNull;

public class Namer extends SchemaObject.Visitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Namer.class);

    private static final Pattern SUBSCHEMA_ID_PATTERN = Pattern.compile("^#[A-Za-z][A-Za-z0-9_:.-]*$");

    private final Diagnostics diagnostics;

    // TODO Namer over a loaded schema

    // The "id" keyword defines a URI for the schema, and the base URI that
    // other URI references within the schema are resolved against.
    // The "id" keyword itself is resolved against the base URI that the object
    // as a whole appears in.
    //

    //

    private final Map<String, SchemaObject> idIndex = new TreeMap<>();

    public Namer(Diagnostics diagnostics) {
        this.diagnostics = Objects.requireNonNull(diagnostics);
    }

    public SchemaObject resolve(URI uri) {
        return idIndex.get(uri.toString());
    }

    // The value of the $ref is a URI
    // Reference. Resolved against the current URI base, it identifies the
    // URI of a schema to use.

    @Override
    public void enterSchema(
                            URI base,
                            String path,
                            String keyword,
                            @NonNull SchemaObject schema) {
        if (isRootSchema(path)) {
            index(base, schema);
        }

        // Explicit id
        String id = schema.getId();
        if (id != null) {
            if (isRootSchema(path)) {
                // Wright 00:
                // The root schema of a JSON Schema document SHOULD contain an "id"
                // keyword with an absolute-URI (containing a scheme, but no fragment).
                URI uri = URI.create(id);
                if (!uri.isAbsolute()) {
                    diagnostics.reportWarning("Root schema of a document should contain an 'id' with an absolute URI, but 'id' is not absolute: {}",
                            base);
                }
                else if (!uri.equals(base)) {
                    index(uri, schema);
                }
            }
            else {
                // Wright 00:
                // To name subschemas in a JSON Schema document, subschemas can use "id"
                // to give themselves a document-local identifier. This form of "id"
                // keyword MUST begin with a hash ("#") to identify it as a fragment URI
                // reference, followed by a letter ([A-Za-z]), followed by any number of
                // letters, digits ([0-9]), hyphens ("-"), underscores ("_"), colons
                // (":"), or periods (".").
                if (!SUBSCHEMA_ID_PATTERN.matcher(id).matches()) {
                    diagnostics.reportError("Invalid schema 'id', must match " + SUBSCHEMA_ID_PATTERN.pattern() + ": " + id);
                }
                index(resolve(base, id), schema);
            }
        }
        else if (isRootSchema(path)) {
            diagnostics.reportWarning("Root schema of a document should contain an 'id' with an absolute URI, but 'id' is absent: {}",
                    base);
        }

        // Pointer id. This cannot collide with the 'id' property because it always begins with /
        // which id is not allowed to contain
        if (!path.isEmpty() && path.indexOf('/') == -1) {
            // Should never happen
            throw new IllegalStateException();
        }
        String pathId = "#" + path;

        index(resolve(base, pathId), schema);

    }

    private void index(URI base, @NonNull SchemaObject schema) {
        SchemaObject old = idIndex.put(base.toString(), schema);
        if (old != null) {
            throw new RuntimeException("Attempt to identify two schemas from same URI " + base);
        }
    }

    private static URI resolve(URI base, String pathId) {
        return base.resolve(pathId);
    }

    @Override
    public void exitSchema(
                           URI base,
                           String path,
                           String keyword,
                           @NonNull SchemaObject schema) {
    }

}
