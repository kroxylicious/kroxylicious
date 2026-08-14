/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.keystore;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException;

/**
 * Shared file permission checks for keystore files.
 */
final class KeystoreFilePermissions {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeystoreFilePermissions.class);
    static final String PERMISSION_CHECK_ENV_VAR = "KROXYLICIOUS_DANGEROUSLY_CHANGE_PERMISSION_CHECK";

    private static final Set<PosixFilePermission> STRICT_INSECURE_PERMISSIONS = EnumSet.of(
            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE);

    private static final Set<PosixFilePermission> RELAXED_INSECURE_PERMISSIONS = EnumSet.of(
            PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE);

    private static final Set<PosixFilePermission> OWNER_ONLY_PERMISSIONS = Set.copyOf(PosixFilePermissions.fromString("rw-------"));

    private KeystoreFilePermissions() {
    }

    static void checkForCredentialStore(Path path) throws CredentialServiceUnavailableException {
        if (!Files.exists(path)) {
            return;
        }
        PosixFileAttributeView posixView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posixView == null) {
            return;
        }
        try {
            Set<PosixFilePermission> perms = posixView.readAttributes().permissions();
            Set<PosixFilePermission> insecure = getInsecurePermissions();
            Set<PosixFilePermission> found = EnumSet.copyOf(insecure);
            found.retainAll(perms);
            if (!found.isEmpty()) {
                throw new CredentialServiceUnavailableException(
                        "KeyStore file " + path + " has insecure permissions: " + PosixFilePermissions.toString(perms) +
                                ". Remove group and world access (e.g. chmod 600).");
            }
        }
        catch (IOException e) {
            throw new CredentialServiceUnavailableException(
                    "Failed to check permissions on KeyStore file: " + path, e);
        }
    }

    static void setOwnerOnly(Path path) throws IOException {
        PosixFileAttributeView posixView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posixView != null) {
            Files.setPosixFilePermissions(path, OWNER_ONLY_PERMISSIONS);
        }
    }

    /**
     * Ensure that a file exists with owner-only permissions before it is written to.
     * On POSIX systems, creates the file atomically with {@code rw-------} permissions if it does not exist.
     * If the file already exists, its permissions are checked and an {@link IOException} is thrown if they
     * are too wide — silently tightening permissions would hide a potential credential exposure.
     * On non-POSIX systems this is a no-op.
     *
     * <p>This method always uses strict owner-only checking and does not consult the
     * {@value #PERMISSION_CHECK_ENV_VAR} environment variable. That variable exists to relax
     * <em>runtime</em> permission checks for the proxy, where the kubelet may mount secret volumes
     * with group-readable permissions that are outside the operator's control. This method is used
     * by the CLI tool which operates on local files before they are uploaded as Kubernetes secrets,
     * so strict owner-only permissions are always appropriate.</p>
     */
    static void ensureOwnerOnlyBeforeWrite(Path path) throws IOException {
        Path parent = path.getParent();
        PosixFileAttributeView posixView = Files.getFileAttributeView(
                parent != null ? parent : path, PosixFileAttributeView.class);
        if (posixView == null) {
            return;
        }

        if (Files.exists(path)) {
            Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
            Set<PosixFilePermission> found = EnumSet.copyOf(STRICT_INSECURE_PERMISSIONS);
            found.retainAll(perms);
            if (!found.isEmpty()) {
                throw new IOException(
                        "Keystore file " + path + " has insecure permissions: " + PosixFilePermissions.toString(perms)
                                + ". It is possible that existing credentials in this file have been read or modified by an unauthorized party."
                                + " Consult your organization's security procedures before continuing — you may need to report this."
                                + " If you accept the risk to continue using the existing credentials,"
                                + " change the file permissions (e.g. with chmod) and retry.");
            }
        }
        else {
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(OWNER_ONLY_PERMISSIONS);
            try {
                Files.createFile(path, attr);
            }
            catch (FileAlreadyExistsException e) {
                Files.setPosixFilePermissions(path, OWNER_ONLY_PERMISSIONS);
            }
        }
    }

    /**
     * Atomically create a new file with owner-only permissions, failing if it already exists.
     * On POSIX systems the file is created with {@code rw-------} permissions.
     * On non-POSIX systems the file is created without explicit permission control.
     *
     * @param path path to the file to create
     * @throws FileAlreadyExistsException if the file already exists
     * @throws IOException if the file cannot be created
     */
    static void createExclusively(Path path) throws IOException {
        Path parent = path.getParent();
        PosixFileAttributeView posixView = Files.getFileAttributeView(
                parent != null ? parent : path, PosixFileAttributeView.class);
        if (posixView != null) {
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(OWNER_ONLY_PERMISSIONS);
            Files.createFile(path, attr);
        }
        else {
            Files.createFile(path);
        }
    }

    private static Set<PosixFilePermission> getInsecurePermissions() {
        String envValue = System.getenv(PERMISSION_CHECK_ENV_VAR);
        if ("0640".equals(envValue)) {
            LOGGER.atWarn()
                    .addKeyValue("envVar", PERMISSION_CHECK_ENV_VAR)
                    .addKeyValue("value", envValue)
                    .log("Relaxed file permission check is active: group-readable files are permitted");
            return RELAXED_INSECURE_PERMISSIONS;
        }
        if (envValue != null && !envValue.isEmpty()) {
            LOGGER.atWarn()
                    .addKeyValue("envVar", PERMISSION_CHECK_ENV_VAR)
                    .addKeyValue("value", envValue)
                    .log("Unrecognized value for permission check env var, using strict default (0600)");
        }
        return STRICT_INSECURE_PERMISSIONS;
    }
}
