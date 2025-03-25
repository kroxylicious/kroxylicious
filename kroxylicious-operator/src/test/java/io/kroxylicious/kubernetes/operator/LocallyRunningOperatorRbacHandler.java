/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchable;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.junit.AbstractOperatorExtension;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Designed to cooperate with {@link io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension}
 * so that the operator runs locally with role based access restrictions as close to that it will
 * have when deployed to Kubernetes for real.
 * <br/>
 * Register this extension before LocallyRunOperatorExtension, passing LocallyRunOperatorExtension
 * the kubernetes client returned by {@link #operatorClient}.  This client will use a principal
 * which has been configured in Kubernetes to have the same RBAC rights as the operator will use
 * in production.
 * <br/>
 * For the test's interactions with Kubernetes, where more privileges are required, use the operations
 * exposed by {@link #testActor(AbstractOperatorExtension)}.
 *
 */
public class LocallyRunningOperatorRbacHandler implements BeforeEachCallback, AfterEachCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocallyRunningOperatorRbacHandler.class);

    private final String impersonatedUser = UUID.randomUUID().toString();

    @AutoClose
    private final KubernetesClient operatorClient = OperatorTestUtils.kubeClient(
            new KubernetesClientBuilder().editOrNewConfig().withImpersonateUsername(impersonatedUser).endConfig());

    @AutoClose
    private final KubernetesClient testActorClient = OperatorTestUtils.kubeClient();

    // @formatter:off
    private final ClusterRole frameworkClusterRole = new ClusterRoleBuilder()
            .withNewMetadata()
                .withName("framework-cluster-role")
            .endMetadata()
            .addNewRule()
                .addToApiGroups("")
                .addToVerbs("get", "create", "delete", "patch")
                .addToResources("namespaces")
            .endRule()
            .addNewRule()
                .addToApiGroups("apiextensions.k8s.io")
                .addToVerbs("get", "list", "watch", "create", "delete", "patch", "delete")
                .addToResources("customresourcedefinitions")
                .endRule()
            .build();
    // @formatter:on

    // @formatter:off
    private final ClusterRoleBinding frameworkClusterRoleBinding = new ClusterRoleBindingBuilder()
            .withNewMetadata()
                .withName("framework-cluster-role-binding")
            .endMetadata()
            .withNewRoleRef()
                .withName(frameworkClusterRole.getMetadata().getName())
                .withKind(frameworkClusterRole.getKind())
                .withApiGroup("rbac.authorization.k8s.io")
            .endRoleRef()
            .addNewSubject()
                .withName(impersonatedUser)
                .withKind("User")
                .withApiGroup("rbac.authorization.k8s.io")
            .endSubject()
            .build();
    // @formatter:on

    @Override
    public void beforeEach(ExtensionContext context) {
        try (var adminClient = OperatorTestUtils.kubeClient();
                var files = Files.list(Path.of("install"))) {
            files.sorted().forEach(resourceFile -> {
                try (var resourceInputStream = new FileInputStream(resourceFile.toString())) {
                    var list = adminClient.load(resourceInputStream).items();
                    list.stream()
                            .filter(this::isClusterRoleOrRoleBindingResource)
                            .map(r -> {
                                // We need to rewrite the RBAC so that rather than being in terms
                                // of a ServiceAccount, they are in terms of our impersonating user.
                                if (r instanceof ClusterRoleBinding crb) {
                                    crb.getSubjects().stream()
                                            .filter(s -> s.getKind().equals("ServiceAccount"))
                                            .forEach(s -> {
                                                var originalUser = s.getName();
                                                s.setKind("User");
                                                s.setNamespace(null);
                                                s.setName(impersonatedUser);
                                                s.setApiGroup("rbac.authorization.k8s.io");
                                                LOGGER.trace("Updated cluster role binding for {} to match impersonated user {}", originalUser,
                                                        impersonatedUser);
                                            });
                                }
                                return r;
                            })
                            .forEach(r -> {
                                LOGGER.trace("Creating/patching: {} from {}", r.getMetadata().getName(), resourceFile);
                                adminClient.resource(r).createOr(EditReplacePatchable::patch);
                            });
                }
                catch (IOException e) {
                    throw new UncheckedIOException("failed to install cluster resources " + resourceFile, e);
                }
            });

            // The test framework itself needs these roles.
            adminClient.resource(frameworkClusterRole).createOr(EditReplacePatchable::patch);
            adminClient.resource(frameworkClusterRoleBinding).createOr(EditReplacePatchable::patch);

            LOGGER.info("Applied Operator RBAC rules rewritten in terms of user {}.", impersonatedUser);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        try (var adminClient = OperatorTestUtils.kubeClient();
                var files = Files.list(Path.of("install"))) {
            files.forEach(resourceFile -> {
                try {
                    adminClient.load(new FileInputStream(resourceFile.toString())).items()
                            .stream().filter(this::isClusterRoleOrRoleBindingResource)
                            .forEach(r -> {
                                LOGGER.trace("Deleting: {} from {}", r.getMetadata().getName(), resourceFile);
                                adminClient.resource(r).delete();
                            });
                }
                catch (FileNotFoundException e) {
                    throw new UncheckedIOException("failed to uninstall cluster resource: " + resourceFile, e);
                }
            });

            adminClient.resource(frameworkClusterRole).delete();
            adminClient.resource(frameworkClusterRoleBinding).delete();
        }

    }

    @NonNull
    public KubernetesClient operatorClient() {
        return operatorClient;
    }

    @NonNull
    public KubernetesClient testActorClient() {
        return testActorClient;
    }

    @NonNull
    public TestActor testActor(@NonNull AbstractOperatorExtension operatorExtension) {
        return new TestActor() {

            @NonNull
            @Override
            public <T extends HasMetadata> T create(@NonNull T resource) {
                return testActorClient.resource(resource).inNamespace(operatorExtension.getNamespace()).create();
            }

            @Nullable
            @Override
            public <T extends HasMetadata> T get(@NonNull Class<T> type, @NonNull String name) {
                return testActorClient.resources(type).inNamespace(operatorExtension.getNamespace()).withName(name).get();
            }

            @NonNull
            public <T extends HasMetadata> T replace(@NonNull T resource) {
                return testActorClient.resource(resource).inNamespace(operatorExtension.getNamespace()).update();
            }

            @Override
            public <T extends HasMetadata> boolean delete(@NonNull T resource) {
                var res = testActorClient.resource(resource).inNamespace(operatorExtension.getNamespace()).delete();
                return res.size() == 1 && res.get(0).getCauses().isEmpty();
            }

            @NonNull
            @Override
            public <T extends HasMetadata> NonNamespaceOperation<T, KubernetesResourceList<T>, Resource<T>> resources(
                                                                                                                      Class<T> type) {
                return testActorClient.resources(type).inNamespace(operatorExtension.getNamespace());
            }

        };
    }

    private boolean isClusterRoleOrRoleBindingResource(HasMetadata r) {
        return r instanceof ClusterRoleBinding || r instanceof ClusterRole;
    }

    public interface TestActor {
        @NonNull
        <T extends HasMetadata> T create(@NonNull T resource);

        @Nullable
        <T extends HasMetadata> T get(@NonNull Class<T> type, @NonNull String name);

        @NonNull
        <T extends HasMetadata> T replace(@NonNull T resource);

        <T extends HasMetadata> boolean delete(@NonNull T resource);

        @NonNull
        <T extends HasMetadata> NonNamespaceOperation<T, KubernetesResourceList<T>, Resource<T>> resources(@NonNull Class<T> type);
    }
}
