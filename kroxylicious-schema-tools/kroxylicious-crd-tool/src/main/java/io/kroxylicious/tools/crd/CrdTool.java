/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.crd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.tools.crd.model.Crd;
import io.kroxylicious.tools.schema.compiler.SchemaCompiler;
import io.kroxylicious.tools.schema.model.SchemaObject;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * <p>Tool which loads CRD-like (see {@link Crd}) declarations from files and processes them into real CRDs
 * which are acceptable to Kubernetes.
 * This permits more flexible and powerful API definitions by:</p>
 * <ul>
 * <li>allowing to separate a Schema from a CRD.</li>
 * <li>allowing use of {@code definitions} and {@code $ref} within the Schema.</li>
 * <li>allowing the reuse of schemas by {@code $ref} (i.e. nominal typing), rather than having to maintain
 * subschemas in alignment (i.e. structural typing).</li>
 * </ul>
 *
 * <p>Recursive type definitions are not allowed, because Kubernetes does not support them.</p>
 */
public class CrdTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(CrdTool.class);

    private final YAMLMapper mapper;
    private final List<Path> srcPaths;
    private final String header;

    public CrdTool(List<Path> srcPaths, String header) {
        this.mapper = new YAMLMapper();
        this.header = header;
        this.srcPaths = srcPaths;
    }

    @NonNull
    private Optional<InputCrd> parse(Path src) {
        try {
            var tree = mapper.readTree(src.toFile());
            if (!tree.isObject()
                    || !"apiextensions.k8s.io/v1".equals(tree.path("apiVersion").asText(""))
                    || !"CustomResourceDefinition".equals(tree.path("kind").asText(""))) {
                LOGGER.warn("Ignoring non-CRD file {} (hint: CRD files should have a top level object, with "
                        + "properties `apiVersion: apiextensions.k8s.io/v1` and `kind: CustomResourceDefinition`)", src);
                return Optional.empty();
            }
            // find CRDs by parsing .yamls (note single doc might contain multiple docs!)
            // check for apiVersion and kind properties
            return Optional.of(new InputCrd(src, mapper.convertValue(tree, Crd.class)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void generateJava(InputCrd inputCrd) {

        // TODO Generate classes according to the CRD schemas
        var schemaCompiler = new SchemaCompiler(List.of(), header, Map.of());
        // schemaCompiler.gen()
        // schemaCompiler.write();
        inputCrd.src();

        // TODO Generate the Crd class itself
        System.out.println(new CrdCodeGen().genDecl("pkg", inputCrd.crd()).toString());
    }

    private void writeCrd(Crd x) {
        try {
            LOGGER.info(mapper.writeValueAsString(x));
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    private Crd flattenCrd(InputCrd input) {
        var crd = input.crd();
        // name (singular, plural, kind, version)
        // subresources
        // schema for each version

        // find schemas (ie. resolve the $ref)
        // TODO flatten the schemas
        for (var version : crd.getSpec().getVersions()) {
            // Base url is the CRD YAML file itself.
            // visit the schema, resolving files in preorder
            // and replacing with the loaded file in post order
            URI base = input.src().toUri();
            Flattener visitor = new Flattener();
            SchemaObject openApiv3Schema = version.getSchema().getOpenAPIV3Schema();
            flattenSchema(base, openApiv3Schema, visitor);
        }
        return crd;
    }

    private static void flattenSchema(URI base, SchemaObject schemaObject, Flattener visitor) {
        schemaObject.visitSchemas(base, visitor);
    }

    private class Flattener extends SchemaObject.Visitor {
        @Override
        public void enterSchema(
                                URI base,
                                String path,
                                SchemaObject schema) {
            if (schema.getRef() != null) {
                var ref = URI.create(schema.getRef());
                if (ref.isAbsolute() || ref.getPath() != null) {
                    // it's a new resource
                    var resolved = base.resolve(ref);
                    if ("file".equals(resolved.getScheme())) {
                        String path1 = resolved.getPath();
                        if (!Files.exists(Path.of(path1))) {
                            throw new RuntimeException("Referenced schema does not exist. referencing schema at " + base + " referencing " + path1);
                        }
                        try {
                            LOGGER.info("Parsing {}", path1);
                            var referred = mapper.readValue(new File(path1), SchemaObject.class);
                            // TODO prevent recursion
                            LOGGER.info("Flattening {}", path1);
                            flattenSchema(resolved, referred, this);
                            LOGGER.info("Referred {}", referred);
                            // System.out.println(referred);
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        // TODO recurse
                    }
                }
                else if (ref.getFragment() != null) {
                    // it's local
                }

            }
        }

        @Override
        public void exitSchema(
                               URI base,
                               String path,
                               @NonNull SchemaObject schema) {
            super.exitSchema(base, path, schema);
            // TODO remove 'id', $schema, etc, so that Kube will actually accept it
        }
    }

    public void run() {
        var l = srcPaths.stream()
                .flatMap(src -> parse(src).stream()).toList();
        l.stream().forEach(this::generateJava);

        // l.stream().map(this::flattenCrd)
        // .forEach(this::writeCrd);
    }

    public static void main(String[] args) {
        var tool = new CrdTool(List.of(Path.of("kroxylicious-schema-tools/kroxylicious-crd-tool/src/test/resources/crd.yaml").toAbsolutePath()),
                null);
        tool.run();
    }
}
