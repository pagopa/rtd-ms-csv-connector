package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundTransactionFieldSetMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ClassUtils;

import java.io.File;
import java.io.FileOutputStream;

/**
 * Class for unit testing of the PGPFlatFileItemReader class
 */
public class PGPFlatFileItemReaderTest extends BaseTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(
            new File(getClass().getResource("/test-encrypt").getFile()));


    public LineTokenizer transactionLineTokenizer() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(";");
        delimitedLineTokenizer.setNames(
                "codice_acquirer", "tipo_operazione", "tipo_circuito", "PAN", "timestamp", "id_trx_acquirer",
                "id_trx_issuer", "correlation_id", "importo", "currency", "acquirerID", "merchantID", "terminal_id",
                "bank_identification_number", "MCC", "par");
        return delimitedLineTokenizer;
    }

    public FieldSetMapper<InboundTransaction> transactionFieldSetMapper(String timestamp) {
        return new InboundTransactionFieldSetMapper(timestamp);
    }

    public LineMapper<InboundTransaction> transactionLineMapper(String timestamp) {
        DefaultLineMapper<InboundTransaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(transactionLineTokenizer());
        lineMapper.setFieldSetMapper(transactionFieldSetMapper(timestamp));
        return lineMapper;
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @SneakyThrows
    @Test
    public void testReader_Ok() {

        File testTrxPgp = tempFolder.newFile("test-trx.pgp");

        FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

        PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                this.getClass().getResource("/test-encrypt").getFile() + "/test-trx.csv",
                PGPDecryptUtil.readPublicKey(
                        this.getClass().getResourceAsStream("/test-encrypt/publicKey.asc")),
                false,false);
        PGPFlatFileItemReader flatFileItemReader = new PGPFlatFileItemReader(
                "file:/"+this.getClass().getResource("/test-encrypt").getFile() +
                        "/secretKey.asc", "test", true);

        textTrxPgpFOS.close();

        flatFileItemReader.setResource(new UrlResource(tempFolder.getRoot().toURI() + "test-trx.pgp"));
        flatFileItemReader.setLineMapper(transactionLineMapper("MM/dd/yyyy HH:mm:ss"));
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        flatFileItemReader.update(executionContext);
        flatFileItemReader.open(executionContext);
        Assert.assertNotNull(flatFileItemReader.read());
        exceptionRule.expect(FlatFileParseException.class);
        flatFileItemReader.read();
        Assert.assertNotNull(flatFileItemReader.read());
        flatFileItemReader.update(executionContext);
        Assert.assertEquals(3, executionContext
                .getInt(ClassUtils.getShortName(FlatFileItemReader.class) + ".read.count"));
    }

    @SneakyThrows
    @Test
    public void testReader_Ok_NoDecrypt() {


        PGPFlatFileItemReader flatFileItemReader = new PGPFlatFileItemReader(
                "file:/"+this.getClass().getResource("/test-encrypt").getFile() +
                        "/secretKey.asc", "test", false);

        flatFileItemReader.setResource(new UrlResource("file:"+
                this.getClass().getResource("/test-encrypt")
                .getFile() + "/test-trx.csv"));
        flatFileItemReader.setLineMapper(transactionLineMapper("MM/dd/yyyy HH:mm:ss"));
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        flatFileItemReader.update(executionContext);
        flatFileItemReader.open(executionContext);
        Assert.assertNotNull(flatFileItemReader.read());
        exceptionRule.expect(FlatFileParseException.class);
        flatFileItemReader.read();
        Assert.assertNotNull(flatFileItemReader.read());
        flatFileItemReader.update(executionContext);
        Assert.assertEquals(3, executionContext
                .getInt(ClassUtils.getShortName(FlatFileItemReader.class) + ".read.count"));
    }

    @SneakyThrows
    @Test
    public void testReader_WrongKey() {

        File testTrxPgp = tempFolder.newFile("test-trx.pgp");

        FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

        PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                this.getClass().getResource("/test-encrypt").getFile() + "/test-trx.csv",
                PGPDecryptUtil.readPublicKey(
                        this.getClass().getResourceAsStream("/test-encrypt/otherPublicKey.asc")),
                false,false);

        textTrxPgpFOS.close();

        PGPFlatFileItemReader flatFileItemReader = new PGPFlatFileItemReader(
                "file:/"+this.getClass().getResource("/test-encrypt").getFile() +
                        "/secretKey.asc", "test", true);
        flatFileItemReader.setResource(new UrlResource(tempFolder.getRoot().toURI() + "test-trx.pgp"));
        flatFileItemReader.setLineMapper(transactionLineMapper("MM/dd/yyyy HH:mm:ss"));
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();
        flatFileItemReader.update(executionContext);
        exceptionRule.expect(ItemStreamException.class);
        flatFileItemReader.open(executionContext);
        Assert.assertEquals(0, executionContext
                .getInt(ClassUtils.getShortName(FlatFileItemReader.class) + ".read.count"));
    }

    @After
    public void tearDown() {
        tempFolder.delete();
    }

}