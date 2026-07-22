/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyStoreException;
import java.util.EnumSet;
import java.util.Set;

import io.kroxylicious.sasl.credentialstore.CredentialServiceUnavailableException;

/**
 * Shared file permission checks for keystore files.
 */
final class KeystoreFilePermissions {

    private static final Set<PosixFilePermission> INSECURE_PERMISSIONS = EnumSet.of(
            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE);

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
            Set<PosixFilePermission> found = EnumSet.copyOf(INSECURE_PERMISSIONS);
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

    static void check(Path path) throws KeyStoreException {
        if (!Files.exists(path)) {
            return;
        }
        PosixFileAttributeView posixView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posixView == null) {
            return;
        }
        try {
            Set<PosixFilePermission> perms = posixView.readAttributes().permissions();
            Set<PosixFilePermission> found = EnumSet.copyOf(INSECURE_PERMISSIONS);
            found.retainAll(perms);
            if (!found.isEmpty()) {
                throw new KeyStoreException(
                        "KeyStore file " + path + " has insecure permissions: " + PosixFilePermissions.toString(perms) +
                                ". Remove group and world access (e.g. chmod 600).");
            }
        }
        catch (IOException e) {
            throw new KeyStoreException(
                    "Failed to check permissions on KeyStore file: " + path, e);
        }
    }

    static void setOwnerOnly(Path path) throws IOException {
        PosixFileAttributeView posixView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (posixView != null) {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"));
        }
    }
}
