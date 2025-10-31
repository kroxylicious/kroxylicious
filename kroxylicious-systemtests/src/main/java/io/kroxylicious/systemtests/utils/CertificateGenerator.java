/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;

public class CertificateGenerator extends KeytoolCertificateGenerator {

    public CertificateGenerator() throws IOException {
        super();
    }

    private String getDomainName(String email, String domain, String organizationUnit, String organization, String city, String state, String country) {
        return "CN=" + domain + ", OU=" + organizationUnit + ", O=" + organization + ", L=" + city + ", ST=" + state + ", C=" + country + ", EMAILADDRESS=" + email;
    }

    private List<String> getSAN(String domain) {
        String sanPrefix = "dns:";
        Pattern pattern = Pattern.compile("^(((?!25?[6-9])[12]\\d|[1-9])?\\d\\.?\\b){4}$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(domain);
        if (matcher.find()) {
            sanPrefix = "IP:";
        }
        return List.of("-ext", "SAN=" + sanPrefix + domain + ",dns:*.svc.cluster.local,dns:lowkey-vault-clusterip.lowkey-vault.svc.cluster.local");
    }

    private boolean isWildcardDomain(String domain) {
        return domain.startsWith("*.");
    }

    @Override
    public void generateSelfSignedCertificateEntry(String email, String domain, String organizationUnit, String organization, String city, String state, String country)
            throws GeneralSecurityException, IOException {
        if (Path.of(this.getKeyStoreLocation()).toFile().exists()) {
            KeyStore keyStore = KeyStore.getInstance(this.getKeyStoreLocation(), this.getPassword());
            keyStore.load(new FileInputStream(this.getKeyStoreLocation()), this.getPassword().toCharArray());
            if (keyStore.containsAlias(domain)) {
                keyStore.deleteEntry(domain);
                keyStore.store(new FileOutputStream(this.getKeyStoreLocation()), this.getPassword().toCharArray());
            }
        }

        List<String> commandParameters = new ArrayList(List.of("keytool", "-genkey"));
        commandParameters.addAll(List.of("-alias", domain));
        commandParameters.addAll(List.of("-keyalg", "RSA"));
        commandParameters.addAll(List.of("-keysize", "2048"));
        commandParameters.addAll(List.of("-sigalg", "SHA256withRSA"));
        commandParameters.addAll(List.of("-storetype", "PKCS12"));
        commandParameters.addAll(List.of("-keystore", this.getKeyStoreLocation()));
        commandParameters.addAll(List.of("-storepass", this.getPassword()));
        commandParameters.addAll(List.of("-keypass", this.getPassword()));
        commandParameters.addAll(List.of("-dname", this.getDomainName(email, domain, organizationUnit, organization, city, state, country)));
        commandParameters.addAll(List.of("-validity", "365"));
        if (this.canGenerateWildcardSAN() && !this.isWildcardDomain(domain)) {
            commandParameters.addAll(getSAN(domain));
        }

        this.runCommand(commandParameters);
        this.createCrtFileToImport(domain);
    }

    private void createCrtFileToImport(String alias) throws IOException {
        List<String> commandParameters = new ArrayList(List.of("keytool", "-export", "-rfc"));
        commandParameters.addAll(List.of("-keystore", this.getKeyStoreLocation()));
        commandParameters.addAll(List.of("-storepass", this.getPassword()));
        commandParameters.addAll(List.of("-storetype", this.getKeyStoreType()));
        commandParameters.addAll(List.of("-alias", alias));
        commandParameters.addAll(List.of("-file", this.getCertFilePath()));
        this.runCommand(commandParameters);
    }

    private void runCommand(List<String> commandParameters) throws IOException {
        ProcessBuilder keytool = (new ProcessBuilder()).command(commandParameters);
        Process process = keytool.start();

        try {
            process.waitFor();
        }
        catch (InterruptedException var6) {
            throw new IOException("Keytool execution error");
        }

        if (process.exitValue() > 0) {
            String processError = (new BufferedReader(new InputStreamReader(process.getErrorStream()))).lines().collect(Collectors.joining(" \\ "));
            String processOutput = (new BufferedReader(new InputStreamReader(process.getInputStream()))).lines().collect(Collectors.joining(" \\ "));
            throw new IOException("Keytool execution error: '" + processError + "', output: '" + processOutput + "', commandline parameters: " + commandParameters);
        }
    }
}
