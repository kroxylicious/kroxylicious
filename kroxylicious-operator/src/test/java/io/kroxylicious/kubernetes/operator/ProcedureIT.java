/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.EnumSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.doxylicious.exec.ExecException;
import io.kroxylicious.doxylicious.exec.ProcExecutor;
import io.kroxylicious.doxylicious.junit5.Procedure;
import io.kroxylicious.doxylicious.junit5.ProcedureTestTemplateExtension;
import io.kroxylicious.doxylicious.junit5.TestProcedure;
import io.kroxylicious.doxylicious.model.ExecDecl;
import io.kroxylicious.doxylicious.model.OutputFileAssertion;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ProcedureIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureIT.class);

    private static void deleteFileTree(Path path) throws IOException {
        LOGGER.info("Deleting file tree {}", path);
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, @Nullable IOException e)
                    throws IOException {
                if (e == null) {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
                else {
                    // directory iteration failed
                    throw e;
                }
            }
        });
    }

    private static void copyFileTree(Path source, Path target) throws IOException {
        LOGGER.info("Coping file tree from {} to {}", source, target);
        Files.walkFileTree(source, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                            throws IOException {
                        Path targetdir = target.resolve(source.relativize(dir));
                        try {
                            Files.copy(dir, targetdir);
                        }
                        catch (FileAlreadyExistsException e) {
                            if (!Files.isDirectory(targetdir)) {
                                throw e;
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.copy(file, target.resolve(source.relativize(file)));
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    private static String imageTag;
    private static Path tempInstall;

    @BeforeAll
    static void beforeAll() throws IOException, ExecException {
        ProcExecutor procExecutor = new ProcExecutor(null);
        procExecutor.exec("", new ExecDecl("pwd", null, null, null, null, null, new OutputFileAssertion(null, null, null, "ERROR")));
        /*
         * GIT_HASH="$(git rev-parse HEAD)"
         * TMP_INSTALL_DIR="$(mktemp -d)"
         * trap 'rm -rf -- "$TMP_INSTALL_DIR"' EXIT
         *
         * info "building operator image in minikube for commit ${GIT_HASH}"
         * IMAGE_TAG="dev-git-${GIT_HASH}"
         * KROXYLICIOUS_VERSION=${KROXYLICIOUS_VERSION:-$(mvn org.apache.maven.plugins:maven-help-plugin:3.4.0:evaluate -Dexpression=project.version -q -DforceStdout)}
         *
         * minikube image build . -f Dockerfile.operator -t quay.io/kroxylicious/operator:${IMAGE_TAG} --build-opt=build-arg=KROXYLICIOUS_VERSION="${KROXYLICIOUS_VERSION}"
         */
        // IMAGE_TAG="dev-git-${GIT_HASH}"
        var kroxyVersion = "0.12.0-SNAPSHOT";
        imageTag = "quay.io/kroxylicious/operator:latest";
        String command = "minikube image build . -f Dockerfile.operator -t %s --build-opt=build-arg=KROXYLICIOUS_VERSION=%s".formatted(
                imageTag,
                kroxyVersion);

        procExecutor.exec("",
                new ExecDecl(Path.of(".."), null, command, null, null, Duration.ofSeconds(120), null, null, new OutputFileAssertion(null, null, null, "ERROR")));
        /*
         * info "installing kroxylicious-operator"
         * cp install/* ${TMP_INSTALL_DIR}
         * ${SED} -i "s|quay.io/kroxylicious/operator:latest|quay.io/kroxylicious/operator:${IMAGE_TAG}|g" ${TMP_INSTALL_DIR}/03.Deployment.kroxylicious-operator.yaml
         */

        tempInstall = Files.createTempDirectory("drvbdr");
        Path source = Path.of("install");
        copyFileTree(source, tempInstall);
    }

    @AfterAll
    static void cleanup() throws IOException, ExecException {
        if (tempInstall != null) {
            deleteFileTree(tempInstall);
        }

        if (imageTag != null) {
            String rmImageCommand = "minikube image rm %s".formatted(imageTag);
            new ProcExecutor(null).exec("", new ExecDecl(rmImageCommand, null, null, null, null, null, null));
        }
    }

    @TestTemplate
    @ExtendWith(ProcedureTestTemplateExtension.class)
    @TestProcedure(value = "deploy_minimal_proxy", assuming = "have_a_kubectl")
    void test(Procedure procedure) {
        procedure.executeProcedure();
        procedure.assertVerification();
    }
}
