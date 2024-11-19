/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.javaparser.ast.CompilationUnit;

import io.kroxylicious.tools.schema.model.SchemaObject;

import edu.umd.cs.findbugs.annotations.NonNull;

public class SchemaCompiler {

    private final List<Path> src;
    private final CodeGen codeGen;
    private final Path dst;
    private final YAMLMapper mapper;

    public SchemaCompiler(List<Path> src, Path dst) {
        this.src = src;
        this.dst = dst;
        this.codeGen = new CodeGen();
        this.mapper = new YAMLMapper();
    }

    public List<Input> parse() {

        return src.stream().flatMap(srcPath -> {
            System.out.println(srcPath.toAbsolutePath());
            try (Stream<Path> walkPaths = Files.walk(srcPath)) {
                return walkPaths.flatMap(file -> {
                    String string = file.getFileName().toString();
                    if (Files.isRegularFile(file)
                            && (string.endsWith(".yaml")
                                    || string.endsWith(".yml")
                                    || string.endsWith(".json"))) {
                        return parserSchema(srcPath, file);
                    }
                    return Stream.empty();
                }).toList().stream(); // Need to materialise this list else the try(walkPaths) will close Stream before anything pulled through the stream
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to walk source directory " + srcPath, e);
            }
        }).toList();
    }

    @NonNull
    private Stream<Input> parserSchema(Path srcPath, Path schemaFile) {
        try {
            var relPath = srcPath.relativize(schemaFile);
            String pkg = StreamSupport.stream(relPath.spliterator(), false).map(Path::toString).collect(Collectors.joining("."));
            System.out.println("Parsing " + schemaFile);
            var tree = mapper.readTree(schemaFile.toFile());
            JsonNode $schema = tree.get("$schema");
            if (!"https://json-schema.org/draft-04/schema".equals($schema.asText("https://json-schema.org/draft-04/schema"))) {
                System.err.println("Ignoring non-schema file: " + schemaFile);
                return Stream.empty();
            }
            var rootSchema = mapper.convertValue(tree, SchemaObject.class);
            return Stream.of(new Input(schemaFile, relPath, pkg, rootSchema));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to read source file " + schemaFile, e);
        }
    }

    public void compile(List<Input> inputs) {
        inputs.stream().forEach(input -> {
            Path javaFile = dst.resolve(input.relPath().getFileName().toString().replaceAll("\\.(json|ya?ml)", ".java"));
            Path parent = javaFile.getParent();
            try {
                Files.createDirectories(parent);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to output dst directory " + parent, e);
            }

            CompilationUnit compilationUnit = codeGen.genDecl(input.pkg(), input.root());
            try {
                Files.writeString(javaFile, compilationUnit.toString());
            }
            catch (IOException e) {
                throw new UncheckedIOException("Unable to output java file " + javaFile, e);
            }
        });

    }

    public static void main(String[] a) {
        var c = new SchemaCompiler(List.of(Path.of("kroxylicious-schema-tools/src/test/schema")),
                Path.of("kroxylicious-schema-tools/target/generated-test-sources/foos"));
        var inputs = c.parse();
        c.compile(inputs);
    }

}
