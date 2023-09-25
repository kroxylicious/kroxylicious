/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.installation.kroxy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import io.kroxylicious.Constants;
import io.kroxylicious.Environment;
import io.kroxylicious.executor.Exec;
import io.kroxylicious.executor.ExecResult;
import io.kroxylicious.k8s.exception.KubeClusterException;
import io.kroxylicious.utils.DeploymentUtils;

/**
 * The type Kroxy.
 */
public class Kroxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(Kroxy.class);
    private final String deploymentNamespace;
    private final String sampleDir;
    private final Path kustomizeTmpdir;

    /**
     * Instantiates a new Kroxy.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Kroxy(String deploymentNamespace, String sampleDir) throws IOException {
        this.deploymentNamespace = deploymentNamespace;
        this.sampleDir = sampleDir;
        kustomizeTmpdir = Files.createTempDirectory(Paths.get("/tmp"), "kustomize", PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwx------")));
    }

    /**
     * Deploy.
     */
    public void deploy() {
        LOGGER.info("Deploy cert manager in cert-manager namespace");
        Exec.exec("kubectl", "apply", "-f", "https://github.com/cert-manager/cert-manager/releases/download/v1.12.0/cert-manager.yaml");
        Exec.exec("kubectl", "wait", "deployment/cert-manager-webhook", "--for=condition=Available=True", "--timeout=300s", "-n", "cert-manager");

        LOGGER.debug("Copying {} directory to {}", sampleDir, kustomizeTmpdir);
        File source = new File(sampleDir);
        File dest = new File(String.valueOf(kustomizeTmpdir));
        try {
            FileUtils.copyDirectory(source, dest);
        }
        catch (IOException e) {
            LOGGER.error(e.getMessage());
        }

        List<File> listFiles = findDirectoriesByName("minikube", kustomizeTmpdir.toFile());
        File overlayDir = !listFiles.isEmpty() ? listFiles.get(0).getAbsoluteFile() : null;
        if (overlayDir == null || !overlayDir.exists()) {
            throw new KubeClusterException.NotFound(new ExecResult(1, "", ""), "Cannot find minikube overlay within sample");
        }

        Exec.exec(overlayDir, "kustomize", "edit", "set", "namespace", deploymentNamespace);
        if (!Objects.equals(Environment.QUAY_ORG, Environment.QUAY_ORG_DEFAULT)) {
            Exec.exec(overlayDir, "kustomize", "edit", "set", "image",
                    "quay.io/kroxylicious/kroxylicious-developer=quay.io/" + Environment.QUAY_ORG + "/kroxylicious");
        }

        LOGGER.info("Deploy Kroxy in {} namespace", deploymentNamespace);
        Exec.exec("kubectl", "apply", "-k", overlayDir.getAbsolutePath());
        Exec.exec("kubectl", "wait", "kafka/my-cluster", "--for=condition=Ready", "--timeout=300s", "-n", deploymentNamespace);
        Exec.exec("kubectl", "wait", "deployment/kroxylicious-proxy", "--for=condition=Available=True", "--timeout=300s", "-n", deploymentNamespace);
    }

    /**
     * Delete.
     */
    public void delete() {
        LOGGER.info("Deleting Kroxy in {} namespace", deploymentNamespace);
        Exec.exec("kubectl", "delete", "-f", "https://github.com/cert-manager/cert-manager/releases/download/v1.12.0/cert-manager.yaml");
        Exec.exec("kubectl", "delete", "pod,svc", "-n", deploymentNamespace, "--all");
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, Constants.KROXY_DEPLOYMENT_NAME);
        kustomizeTmpdir.toFile().deleteOnExit();
    }

    private static List<File> findDirectoriesByName(String name, File root) {
        List<File> result = new ArrayList<>();

        for (File file : root.listFiles()) {
            if (file.isDirectory()) {
                if (file.getName().equals(name)) {
                    result.add(file);
                }

                result.addAll(findDirectoriesByName(name, file));
            }
        }

        return result;
    }
}
