package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.batch.encryption.exception.PGPDecryptException;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;

/**
 * Extension of the FlatFileItemReader, in which a decrcyption phase is
 * added to extract the csv content from the .pgp files
 */

@RequiredArgsConstructor
public class PGPFlatFileItemReader extends FlatFileItemReader<InboundTransaction> {

    private final String secretFilePath;
    private final String passphrase;

    private Resource resource;

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
        super.setResource(resource);
    }

    @SneakyThrows
    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
        File fileToProcess = resource.getFile();
        FileInputStream fileToProcessIS = new FileInputStream(fileToProcess);
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource secretKeyResource = resolver.getResource(secretFilePath);
        FileInputStream secretFilePathIS = new FileInputStream(secretKeyResource.getFile());
        try {
            byte[] decryptFileData = PGPDecryptUtil.decryptFile(
                    fileToProcessIS,
                    secretFilePathIS,
                    passphrase.toCharArray()
            );
            super.setResource(new InputStreamResource(new ByteArrayInputStream(decryptFileData)));
        } catch (Exception e) {
            throw new PGPDecryptException();
        } finally {
            fileToProcessIS.close();
            secretFilePathIS.close();
        }
        super.doOpen();

    }


}