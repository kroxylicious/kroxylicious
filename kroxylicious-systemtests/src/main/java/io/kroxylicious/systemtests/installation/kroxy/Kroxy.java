/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

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
     * @param sampleDir the sample dir
     * @throws IOException the io exception
     */
    public Kroxy(String deploymentNamespace, String sampleDir) throws IOException {
        this.deploymentNamespace = deploymentNamespace;
        this.sampleDir = sampleDir;
        kustomizeTmpdir = Files.createTempDirectory(Paths.get("/tmp"), "kustomize", PosixFilePermissions.asFileAttribute(
                PosixFilePermissions.fromString("rwxr--r--")));
    }

    /**
     * Deploy.
     * @throws IOException the io exception
     */
    public void deploy() throws IOException {
        LOGGER.info("Deploy cert manager in {} namespace", Constants.CERT_MANAGER_NAMESPACE);
        kubeClient().getClient().load(DeploymentUtils.getDeploymentFileFromURL(Constants.CERT_MANAGER_URL)).create();
        DeploymentUtils.waitForDeploymentReady(Constants.CERT_MANAGER_NAMESPACE, "cert-manager-webhook");

        LOGGER.debug("Copying {} directory to {}", sampleDir, kustomizeTmpdir);
        File source = new File(sampleDir);
        File dest = new File(String.valueOf(kustomizeTmpdir));
        FileUtils.copyDirectory(source, dest);

        List<File> listFiles = TestUtils.findDirectoriesByName("minikube", kustomizeTmpdir.toFile());
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
        DeploymentUtils.waitForDeploymentReady(deploymentNamespace, Constants.KROXY_DEPLOYMENT_NAME);
        DeploymentUtils.waitForPodToBeReadyByLabel(deploymentNamespace, "strimzi.io/name", "my-cluster-kafka");
    }

    /**
     * Delete.
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        LOGGER.info("Deleting Kroxy in {} namespace", deploymentNamespace);
        kubeClient().getClient().load(DeploymentUtils.getDeploymentFileFromURL(Constants.CERT_MANAGER_URL)).withGracePeriod(0).delete();
        kubeClient().getClient().pods().inNamespace(deploymentNamespace).withGracePeriod(0).delete();
        kubeClient().getClient().services().inNamespace(deploymentNamespace).withGracePeriod(0).delete();
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, Constants.KROXY_DEPLOYMENT_NAME);
        kustomizeTmpdir.toFile().deleteOnExit();
    }
}
