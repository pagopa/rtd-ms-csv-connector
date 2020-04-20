package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.decrypt.PGPDecryptUtil;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RequiredArgsConstructor
public class PGPItemReader implements ItemReader<byte[]>, InitializingBean {

    private final String directoryPath;
    private final String secretFilePath;
    private final String passphrase;

    private final List<File> foundFiles = Collections.synchronizedList(new ArrayList<>());

    @SneakyThrows
    @Override
    public byte[] read() {

        File fileToProcess = null;

        synchronized (foundFiles) {
            if (!foundFiles.isEmpty()) {
                fileToProcess = foundFiles.remove(0);
            }
        }

        return fileToProcess != null ?
                PGPDecryptUtil.decryptFile(
                        new FileInputStream(fileToProcess),
                        new FileInputStream(secretFilePath),
                        passphrase.toCharArray()
                ) :
                null;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        for (final Resource file : getFiles()) {
            this.foundFiles.add(file.getFile());
        }
    }

    private Resource[] getFiles() throws IOException {
        ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        return patternResolver.getResources(directoryPath);
    }

}