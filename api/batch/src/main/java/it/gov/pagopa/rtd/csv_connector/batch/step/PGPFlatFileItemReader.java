package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.batch.encryption.exception.PGPDecryptException;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;

/**
 * Extension of {@link FlatFileItemReader}, in which a decryption phase is
 * added to extract the csv content from the .pgp files
 */

@RequiredArgsConstructor
@Slf4j
public class PGPFlatFileItemReader extends TransactionFlatFileItemReader {

    private final String secretFilePath;
    private final String passphrase;
    private final Boolean applyDecrypt;

    private Resource resource;

    @Override
    public void setResource(Resource resource) {
        this.resource = resource;
        super.setResource(resource);
    }

    /**
     * Override of {@link FlatFileItemReader#doOpen},introduces a
     * decrypt pass before calling on the parent implementation
     *
     * @throws Exception
     */
    @SneakyThrows
    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
        if (applyDecrypt) {
            File fileToProcess = resource.getFile();
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource secretKeyResource = resolver.getResource(secretFilePath);
            try (FileInputStream fileToProcessIS = new FileInputStream(fileToProcess);
                FileInputStream secretFilePathIS = new FileInputStream(secretKeyResource.getFile())) {
                try {
                    byte[] decryptFileData = PGPDecryptUtil.decryptFile(
                            fileToProcessIS,
                            secretFilePathIS,
                            passphrase.toCharArray()
                    );
                    super.setResource(new InputStreamResource(new ByteArrayInputStream(decryptFileData)));
                } catch (Exception e) {
                    log.error(e.getMessage(),e);
                    throw new PGPDecryptException(e.getMessage(),e);
                }
            }
        }
        super.doOpen();

    }


}