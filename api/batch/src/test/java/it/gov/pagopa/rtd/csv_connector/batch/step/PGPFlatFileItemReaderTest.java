package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundTransactionFieldSetMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.batch.item.ExecutionContext;
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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PGPFlatFileItemReaderTest extends BaseTest {

    Path resourceDirectory = Paths.get("src","test","resources");
    String resourcePath = resourceDirectory.toAbsolutePath().toString();

    @SneakyThrows
    @Before
    public void setUp() {
        String resourcePath = resourceDirectory.toAbsolutePath().toString();
        new File(resourcePath + "/test-encrypt/test-trx.pgp").createNewFile();
        PGPDecryptUtil.encryptFile(new FileOutputStream(resourcePath + "/test-encrypt/test-trx.pgp"),
                resourcePath + "/test-encrypt/test-trx.csv",
                PGPDecryptUtil.readPublicKey(new FileInputStream(resourcePath +"/test-encrypt/publicKey.asc")),
                false,false);
    }

    public LineTokenizer transactionLineTokenizer() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(";");
        delimitedLineTokenizer.setNames(
                "codice_acquirer", "tipo_operazione", "tipo_circuito", "PAN", "timestamp", "id_trx_acquirer",
                "id_trx_issuer", "correlation_id", "importo", "currency", "acquirerID", "merchantID", "MCC");
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
    public void testReader() {
        PGPFlatFileItemReader flatFileItemReader = new PGPFlatFileItemReader(
                resourcePath +"/test-encrypt/secretKey.asc", "test");
        flatFileItemReader.setResource(new UrlResource("file:/"+resourcePath+"/test-encrypt/test-trx.pgp"));
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

}