/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema;

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
        codeGen = new CodeGen(diagnostics, new Namer(diagnostics));
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

    private void assertGeneratedCode(String yamlFilename) throws IOException {
        // First assert that the generate code matches the expected files
        assertGeneratedCodeMatches(yamlFilename);
        // Then assert that the expected files can be compiled with a java compiler
        // Because `generated == expected` this means the generated must be legal java source code
        compileJavaFilesBeneath(Path.of(yamlFilename).getParent());
    }

    private static void assertGeneratedCodeMatches(String yamlFilename) {
        File src = new File(yamlFilename);
        SchemaCompiler schemaCompiler = new SchemaCompiler(List.of(new File("src/test/resources").toPath()), null);
        List<Input> parse = schemaCompiler.parse();
        var trees = parse.stream()
                // This is ugly. Currently the compiler doesn't accept a list of files to compile,
                // just a root src. The tests are all in one root dir, so parse() returns all the YAMLs.
                // This filter narrows down to just those YAMLs in the same dir as the yamlFilename
                .filter(input -> input.schemaPath().getParent().endsWith(Path.of(yamlFilename).getParent()))
                .toList();
        var units = schemaCompiler.gen(trees).toList();
        Map<String, List<CompilationUnit>> collect = units.stream().collect(Collectors.groupingBy(SchemaCompiler::javaFileName));
        assertThat(collect).hasKeySatisfying(new Condition<>(filename -> filename.matches("[A-Z][a-zA-Z0-9_$]*\\.java"), "Valid .java filename"));
        assertThat(collect).hasValueSatisfying(new Condition<>(unitsForFile -> unitsForFile.size() == 1, "No colliding units"));
        collect.forEach((javaFilename, cus) -> {
            File expectedJavaFile = new File(src.getParentFile(), javaFilename);
            assertThat(expectedJavaFile)
                    .describedAs("Unexpected java source output (or expected output java file doesn't exist)")
                    .exists();
            try {
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

    @Test
    void empty() throws IOException {
        String pathname = "src/test/resources/empty/Empty.yaml";
        assertGeneratedCode(pathname);
    }

    @Test
    void scalarProperties() throws IOException {
        String pathname = "src/test/resources/scalars/ScalarProperties.yaml";
        assertGeneratedCode(pathname);
    }

    @Test
    void arrays() throws IOException {
        String pathname = "src/test/resources/arrays/Arrays.yaml";
        assertGeneratedCode(pathname);
    }

    @Test
    void maps() throws IOException {
        String pathname = "src/test/resources/maps/Maps.yaml";
        assertGeneratedCode(pathname);
    }

    @Test
    void anonymous() throws IOException {
        String pathname = "src/test/resources/anonymous/Anonymous.yaml";
        assertGeneratedCode(pathname);
    }

    @Test
    void xref() throws IOException {
        String pathname = "src/test/resources/xref/Xref.yaml";
        assertGeneratedCode(pathname);
    }

}
