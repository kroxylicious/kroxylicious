/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.github.javaparser.ast.CompilationUnit;

import io.kroxylicious.tools.schema.model.SchemaObject;
import io.kroxylicious.tools.schema.model.SchemaObjectBuilder;
import io.kroxylicious.tools.schema.model.SchemaType;
import io.kroxylicious.tools.schema.model.XKubeListType;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class CodeGenTest {

    CodeGen codeGen;

    {
        Diagnostics diagnostics = new Diagnostics();
        codeGen = new CodeGen(diagnostics, new IdVisitor(diagnostics), Map.of(),
                "edu.umd.cs.findbugs.annotations.Nullable",
                "edu.umd.cs.findbugs.annotations.NonNull");
    }

    SchemaObject emptyTypes = new SchemaObjectBuilder().withType().build();
    SchemaObject nullSchema = new SchemaObjectBuilder().withType(SchemaType.NULL).build();
    SchemaObject booleanSchema = new SchemaObjectBuilder().withType(SchemaType.BOOLEAN).build();
    SchemaObject stringSchema = new SchemaObjectBuilder().withDescription("A string").withType(SchemaType.STRING).build();
    SchemaObject integerSchema = new SchemaObjectBuilder().withType(SchemaType.INTEGER).build();
    SchemaObject numberSchema = new SchemaObjectBuilder().withType(SchemaType.NUMBER).build();
    SchemaObject emptyObjectSchema = new SchemaObjectBuilder().withType(SchemaType.OBJECT).withJavaType("EmptyObject").build();
    SchemaObject stringArrayListSchema = new SchemaObjectBuilder().withType(SchemaType.ARRAY).withItems(stringSchema).build();
    SchemaObject integerArrayListSchema = new SchemaObjectBuilder().withType(SchemaType.ARRAY).withItems(integerSchema).build();
    SchemaObject stringArraySetSchema = new SchemaObjectBuilder(stringArrayListSchema).withXKubernetesListType(XKubeListType.SET).build();

    @Test
    void genTypeName() {
        String pkg = "foo";
        assertThat(codeGen.genTypeName(pkg, null, emptyTypes)).hasToString("java.lang.Object");
        assertThat(codeGen.genTypeName(pkg, null, nullSchema)).hasToString("java.lang.Object");
        assertThat(codeGen.genTypeName(pkg, null, booleanSchema)).hasToString("java.lang.Boolean");
        assertThat(codeGen.genTypeName(pkg, null, stringSchema)).hasToString("java.lang.String");
        assertThat(codeGen.genTypeName(pkg, null, integerSchema)).hasToString("java.lang.Long");
        assertThat(codeGen.genTypeName(pkg, null, numberSchema)).hasToString("java.lang.Double");
        assertThat(codeGen.genTypeName(pkg, null, stringArrayListSchema)).hasToString("java.util.List<java.lang.String>");
        assertThat(codeGen.genTypeName(pkg, null, integerArrayListSchema)).hasToString("java.util.List<java.lang.Long>");
        assertThat(codeGen.genTypeName(pkg, null, stringArraySetSchema)).hasToString("java.util.Set<java.lang.String>");
        assertThat(codeGen.genTypeName(pkg, null, emptyObjectSchema)).hasToString("foo.EmptyObject");
    }

    private static final String HEADER = """
            /*
             * Copyright Kroxylicious Authors.
             *
             * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
             */

            """;

    private void assertGeneratedCode(String dir) throws IOException {
        // First assert that the generate code matches the expected files
        assertGeneratedCodeMatches(dir);
        // Then assert that the expected files can be compiled with a java compiler
        // Because `generated == expected` this means the generated must be legal java source code
        compileJavaFilesBeneath(Path.of(dir).getParent());
        javadocJavaFilesBeneath(Path.of(dir).getParent());
    }

    private static SchemaCompiler parseDiagnostics(String dir) {
        File src = new File(dir);
        Path path = new File("src/test/resources").toPath();
        SchemaCompiler schemaCompiler = new SchemaCompiler(
                List.of(path),
                List.of(path.relativize(src.toPath()).toString().replace("/", ".")),
                null,
                Map.of());
        schemaCompiler.parse();
        return schemaCompiler;
    }

    private static SchemaCompiler genDiagnostics(String dir) {
        File src = new File(dir);
        Path path = new File("src/test/resources").toPath();
        SchemaCompiler schemaCompiler = new SchemaCompiler(
                List.of(path),
                List.of(path.relativize(src.toPath()).toString().replace("/", ".")),
                null,
                Map.of());
        List<SchemaInput> parse = schemaCompiler.parse();
        assertThat(schemaCompiler.diagnostics.getNumFatals()).isZero();
        assertThat(schemaCompiler.diagnostics.getNumErrors()).isZero();
        assertThat(schemaCompiler.diagnostics.getNumWarnings()).isZero();
        try {
            schemaCompiler.gen(parse).toList();
        }
        catch (FatalException e) {
            // pass
        }
        return schemaCompiler;
    }

    private static void assertGeneratedCodeMatches(String dir) {
        Path src = Path.of(dir);
        Path path = new File("src/test/resources").toPath();
        SchemaCompiler schemaCompiler = new SchemaCompiler(
                List.of(path),
                List.of(path.relativize(src).toString().replace("/", ".")),
                null,
                Map.of());
        List<SchemaInput> parse = schemaCompiler.parse();
        var units = schemaCompiler.gen(parse).toList();

        assertThat(schemaCompiler.diagnostics.getNumFatals()).describedAs("Expect 0 fatal errors").isZero();
        assertThat(schemaCompiler.diagnostics.getNumErrors()).describedAs("Expect 0 errors").isZero();
        // TODO assertThat(schemaCompiler.diagnostics.getNumWarnings()).describedAs("Expect 0 warnings").isZero();

        Map<String, List<CompilationUnit>> collect = units.stream().collect(Collectors.groupingBy(SchemaCompiler::javaFileName));
        assertThat(collect).hasKeySatisfying(new Condition<>(
                filename -> filename.matches("[A-Z][a-zA-Z0-9_$]*\\.java"),
                "Valid .java filename"));
        assertThat(collect).hasValueSatisfying(new Condition<>(
                unitsForFile -> unitsForFile.size() == 1,
                "No colliding units"));
        collect.forEach((javaFilename, cus) -> {
            File expectedJavaFile = new File(src.toFile(), javaFilename);
            assertThat(expectedJavaFile)
                    .describedAs("Unexpected java source output (or expected output java file doesn't exist)")
                    .exists();
            try {
                // The following can be uncomments to bulk-update the expected java files
                // following a change to the code generator
                // USE WITH CAUTION ;-)
                //Files.writeString(expectedJavaFile.toPath(), HEADER + cus.get(0).toString());
                String javaSrc = Files.readString(expectedJavaFile.toPath()).trim();
                assertThat(cus).singleElement()
                        .isNotNull()
                        .extracting(compilationUnit -> HEADER + compilationUnit.toString().trim())
                        .describedAs("Java source output differs from expected output in " + expectedJavaFile)
                        .isEqualTo(javaSrc);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

    }

    /**
     * Compile the *.java files found beneath the given path.
     * Throw away the generated .class files
     * @param path
     * @throws IOException
     */
    private static void compileJavaFilesBeneath(Path path) throws IOException {
        var outputDir = Files.createTempDirectory(CodeGenTest.class.getSimpleName());
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        try (var fileManager = compiler.getStandardFileManager(null, null, null)) {
            Iterable<? extends JavaFileObject> compilationUnits1 = fileManager.getJavaFileObjectsFromPaths(javaFilesBeneath(path));
            assertThat(compiler.getTask(null, fileManager, null, List.of("-d", outputDir.toString()), null, compilationUnits1).call())
                    .describedAs("The java source code should compile without errors")
                    .isTrue();
        }
    }

    /**
     * Compile the *.java files found beneath the given path.
     * Throw away the generated .class files
     * @param path
     * @throws IOException
     */
    private static void javadocJavaFilesBeneath(Path path) throws IOException {
        var outputDir = Files.createTempDirectory(CodeGenTest.class.getSimpleName());
        var docTool = ToolProvider.getSystemDocumentationTool();
        try (var fileManager = docTool.getStandardFileManager(null, null, null)) {
            Iterable<? extends JavaFileObject> compilationUnits1 = fileManager.getJavaFileObjectsFromPaths(javaFilesBeneath(path));
            assertThat(
                    docTool.getTask(null, fileManager, null, null, List.of("-Xdoclint:all", "-Werror", "-public", "-d", outputDir.toString()), compilationUnits1).call())
                    .describedAs("The javadoc should be processed without errors")
                    .isTrue();
        }
    }

    @NonNull
    private static ArrayList<Path> javaFilesBeneath(Path start) throws IOException {
        var result = new ArrayList<Path>();
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(
                                             Path file,
                                             BasicFileAttributes attrs)
                    throws IOException {
                if (file.toString().endsWith(".java")) {
                    result.add(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return result;
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "src/test/resources/empty",
            "src/test/resources/scalars",
            "src/test/resources/arrays",
            "src/test/resources/maps",
            "src/test/resources/anonymous",
            "src/test/resources/trickynaming",
            "src/test/resources/xref",
            "src/test/resources/junctor"
    })
    void compiles(String dir) throws IOException {
        assertGeneratedCode(dir);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "src/test/resources/nonschemafile",
            "src/test/resources/badschemaversion"
    })
    void warnings(String pathname) {
        assertThat(parseDiagnostics(pathname))
                .satisfies(schemaCompiler -> {
                    assertThat(schemaCompiler.numFatals()).isZero();
                    assertThat(schemaCompiler.numErrors()).isZero();
                    assertThat(schemaCompiler.numWarnings()).isEqualTo(1);
                });
    }

    @Test
    void unionTypeIsFatal() {
        String pathname = "src/test/resources/uniontype";
        assertThat(genDiagnostics(pathname))
                .satisfies(schemaCompiler -> {
                    assertThat(schemaCompiler.numFatals()).isEqualTo(1);
                    assertThat(schemaCompiler.numErrors()).isZero();
                    assertThat(schemaCompiler.numWarnings()).isZero();
                });
    }

}
