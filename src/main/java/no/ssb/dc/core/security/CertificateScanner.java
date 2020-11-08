package no.ssb.dc.core.security;

import no.ssb.dapla.secrets.api.SecretManagerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static no.ssb.dapla.secrets.api.SecretManagerClient.safeCharArrayAsUTF8;

class CertificateScanner {

    private static final Logger LOG = LoggerFactory.getLogger(CertificateScanner.class);

    final Path scanDir;
    Map<String, CertificateBundle> bundles;

    CertificateScanner(Path scanDir) {
        this.scanDir = scanDir;
        if (LOG.isTraceEnabled()) {
            LOG.trace("Scan directory: {}", scanDir.toAbsolutePath());
        }
    }

    Map<String, CertificateBundle> getCertificateBundles() {
        Objects.requireNonNull(bundles);
        return bundles;
    }

    void scan() {
        try {
            bundles = findSecurityPropertyFilesAndLoad();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, CertificateBundle> findSecurityPropertyFilesAndLoad() throws IOException {
        Map<String, CertificateBundle> certificateBundles = new LinkedHashMap<>();
        Files.walkFileTree(scanDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!Files.isDirectory(file) && file.getFileName().toAbsolutePath().endsWith("secret.properties")) {
                    Path fileNamePath = file.toAbsolutePath();
                    try (SecretManagerClient secretsClient = SecretManagerClient.create(Map.of(
                            "secrets.provider", "safe-configuration",
                            "secrets.propertyResourcePath", fileNamePath.toString()
                    ))) {
                        CertificateBundle.Builder builder = new CertificateBundle.Builder();

                        builder.secretFileNamePath(fileNamePath);

                        String bundleName = file.getParent().getName(file.getParent().getNameCount() - 1).toString();

                        if (certificateBundles.containsKey(bundleName)) {
                            LOG.warn("The certificate bundle '{}' is already discovered and loaded from: {}", bundleName, certificateBundles.get(bundleName).secretPropertiesPath().orElseThrow());
                            return FileVisitResult.TERMINATE;
                        }

                        boolean isPEMBundle = secretsClient.readBytes("private.key") != null && secretsClient.readBytes("public.certificate") != null;
                        boolean isP12Bundle = secretsClient.readBytes("archive.certificate") != null;

                        if (isPEMBundle) {
                            if (validateSecretProperties(secretsClient, file, "secret.passphrase", "private.key", "public.certificate")) {
                                return FileVisitResult.TERMINATE;
                            }

                            Path privateKeyFile = file.getParent().resolve(secretsClient.readString("private.key"));
                            if (!privateKeyFile.toFile().exists()) {
                                LOG.warn("Skipping bundle '{}'. Could not find privateKeyFile: {}", bundleName, privateKeyFile.toAbsolutePath());
                                return FileVisitResult.TERMINATE;
                            }
                            builder.privateKey(readFileToCharArray(privateKeyFile));

                            Path publicCertFile = file.getParent().resolve(secretsClient.readString("public.certificate"));
                            if (!publicCertFile.toFile().exists()) {
                                LOG.warn("Skipping bundle '{}'. Could not find publicCertFile: {}", bundleName, publicCertFile.toAbsolutePath());
                                return FileVisitResult.TERMINATE;
                            }
                            builder.publicCert(readFileToCharArray(publicCertFile));

                        } else if (isP12Bundle) {
                            if (validateSecretProperties(secretsClient, file, "secret.passphrase", "archive.certificate")) {
                                return FileVisitResult.TERMINATE;
                            }

                            Path archiveCertFile = file.getParent().resolve(secretsClient.readString("archive.certificate"));
                            if (!archiveCertFile.toFile().exists()) {
                                LOG.warn("Skipping bundle '{}'. Could not find archiveCertFile: {}", bundleName, archiveCertFile.toAbsolutePath());
                                return FileVisitResult.TERMINATE;
                            }
                            builder.archiveCert(Files.readAllBytes(archiveCertFile));

                        } else {
                            LOG.warn("Skipping bundle '{}'. Error with configuration!", bundleName);
                        }

                        builder.passphrase(safeCharArrayAsUTF8(secretsClient.readBytes("secret.passphrase")));

                        certificateBundles.put(bundleName, builder.build());
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return certificateBundles;
    }

    private boolean validateSecretProperties(SecretManagerClient secretsClient, Path file, String... secretProperties) {
        for (String secretProperty : secretProperties) {
            if (secretsClient.readBytes(secretProperty) == null) {
                LOG.warn("Undefined property '{}' in 'secret.passphrase'. Skip loading certificate bundle for: {}", secretProperty, file.toAbsolutePath());
                return true;
            }
        }
        return false;
    }

    private char[] readFileToCharArray(Path file) throws IOException {
        FileReader fr = new FileReader(file.toAbsolutePath().toString());
        int count;
        int size = (int) file.toFile().length();
        char[] buffer = new char[size];
        do {
            count = fr.read(buffer);
        } while (count != -1);
        return buffer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CertificateScanner scanner = (CertificateScanner) o;
        return Objects.equals(scanDir, scanner.scanDir) &&
                Objects.equals(bundles, scanner.bundles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scanDir, bundles);
    }

    @Override
    public String toString() {
        return "CertificateScanner{" +
                "scanDir=" + scanDir +
                ", bundles=" + bundles +
                '}';
    }

}
