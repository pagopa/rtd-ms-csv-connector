package it.gov.pagopa.rtd.csv_connector.batch;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootTest(classes = PGPDecryptUtil.class)
@RunWith(SpringRunner.class)
public class CsvTransactionReaderBatchTest {

    //FIXME: Temporary usage of encrypting method for testing purposes, to be removed

    @SneakyThrows
    @Before
    public void encrypt() {
        Path resourceDirectory = Paths.get("src","test","resources");
        String resourcePath = resourceDirectory.toAbsolutePath().toString();
        new File(resourcePath + "/test-encrypt/test-trx.pgp").createNewFile();
        PGPDecryptUtil.encryptFile(new FileOutputStream(resourcePath + "/test-encrypt/test-trx.pgp"),
                resourcePath + "/test-encrypt/test-trx.csv",
                PGPDecryptUtil.readPublicKey(new FileInputStream(resourcePath +"/test-encrypt/publicKey.asc")),
                false,false);
    }

    @Test
    public void test() {}

}