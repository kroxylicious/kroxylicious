/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.testkafkacluster;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeytoolCertificateGenerator {
    private String password;
    private Path certsDirectory;
    private Path certFilePath;
    private final Logger log = LoggerFactory.getLogger(KeytoolCertificateGenerator.class.getName());

    public KeytoolCertificateGenerator() throws IOException {
        certsDirectory = Files.createTempDirectory("kproxy");
        certsDirectory.toFile().deleteOnExit();
        certFilePath = Paths.get(certsDirectory.toAbsolutePath().toString(), "kafka.jks");
    }

    public String getCertLocation() {
        return certFilePath.toAbsolutePath().toString();
    }

    public String getPassword() {
        if (password == null) {
            password = UUID.randomUUID().toString().replace("-", "");
        }
        return password;
    }

    public boolean canGenerateWildcardSAN() {
        return Runtime.version().feature() >= 17;
    }

    public void generateSelfSignedCertificateEntry(String email, String domain, String organizationUnit,
                                                   String organization, String city, String state,
                                                   String country)
            throws GeneralSecurityException, IOException {

        KeyStore keyStore = KeyStore.getInstance("JKS");
        if (certFilePath.toFile().exists()) {
            keyStore.load(new FileInputStream(certFilePath.toFile()), getPassword().toCharArray());

            if (keyStore.containsAlias(domain)) {
                keyStore.deleteEntry(domain);
                keyStore.store(new FileOutputStream(certFilePath.toFile()), getPassword().toCharArray());
            }
        }

        final List<String> commandParameters = new ArrayList<>(List.of("keytool", "-genkey"));
        commandParameters.addAll(List.of("-alias", domain));
        commandParameters.addAll(List.of("-keyalg", "RSA"));
        commandParameters.addAll(List.of("-keysize", "2048"));
        commandParameters.addAll(List.of("-sigalg", "SHA256withRSA"));
        commandParameters.addAll(List.of("-storetype", "JKS"));
        commandParameters.addAll(List.of("-keystore", getCertLocation()));
        commandParameters.addAll(List.of("-storepass", getPassword()));
        commandParameters.addAll(List.of("-keypass", getPassword()));
        commandParameters.addAll(
                List.of("-dname", getDomainName(email, domain, organizationUnit, organization, city, state, country)));
        commandParameters.addAll(List.of("-validity", "365"));
        commandParameters.addAll(List.of("-deststoretype", "pkcs12"));
        commandParameters.addAll(List.of("-storetype", "JKS"));
        if (canGenerateWildcardSAN() && !isWildcardDomain(domain)) {
            commandParameters.addAll(getSAN(domain));
        }
        ProcessBuilder keytool = new ProcessBuilder().command(commandParameters);

        final Process process = keytool.start();
        try {
            process.waitFor();
        }
        catch (InterruptedException e) {
            throw new IOException("Keytool execution error");
        }

        log.info("Generating certificate using `keytool` using command: " + process.info() + ", parameters: " +
                commandParameters);

        if (process.exitValue() > 0) {
            final String processError = (new BufferedReader(new InputStreamReader(process.getErrorStream()))).lines()
                    .collect(Collectors.joining(" \\ "));
            final String processOutput = (new BufferedReader(new InputStreamReader(process.getInputStream()))).lines()
                    .collect(Collectors.joining(" \\ "));
            log.warn("Error generating certificate, error output: " + processError + ", normal output: " +
                    processOutput + ", commandline parameters: " + commandParameters);
            throw new IOException(
                    "Keytool execution error: '" + processError + "', output: '" + processOutput + "'" + ", commandline parameters: " + commandParameters);
        }
    }

    private boolean isWildcardDomain(String domain) {
        return domain.startsWith("*.");
    }

    private String getDomainName(String email, String domain, String organizationUnit, String organization, String city,
                                 String state, String country) {
        // keytool doesn't allow for a domain with wildcard in SAN extension, so it has to go directly to CN
        return "CN=" + domain + ", OU=" + organizationUnit + ", O=" + organization + ", L=" + city + ", ST=" + state +
                ", C=" + country + ", EMAILADDRESS=" + email;
    }

    private List<String> getSAN(String domain) {
        return List.of("-ext", "SAN=dns:" + domain);
    }
}
