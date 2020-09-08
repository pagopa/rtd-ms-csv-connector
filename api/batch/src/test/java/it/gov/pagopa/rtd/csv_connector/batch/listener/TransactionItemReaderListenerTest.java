package it.gov.pagopa.rtd.csv_connector.batch.listener;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class TransactionItemReaderListenerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(
            new File(getClass().getResource("/test-encrypt").getFile()));

    @SneakyThrows
    @Test
    public void beforeStep_OK() {

        File folder = tempFolder.newFolder("testListener");
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
        String executionDate = OffsetDateTime.now().format(fmt);

        TransactionItemReaderListener transactionItemReaderListener = new TransactionItemReaderListener();
        transactionItemReaderListener.setExecutionDate(executionDate);
        transactionItemReaderListener.setResolver(new PathMatchingResourcePatternResolver());
        transactionItemReaderListener.setErrorTransactionsLogsPath("file:/"+folder.getAbsolutePath());
        transactionItemReaderListener.afterRead(InboundTransaction
                .builder().filename("test").lineNumber(1).build());

        Assert.assertEquals(0,
                FileUtils.listFiles(
                        resolver.getResources("classpath:/test-encrypt/**/testListener")[0].getFile(),
                        new String[]{"csv"},false).size());

    }

    @SneakyThrows
    @Test
    public void onReadError_OK() {

        File folder = tempFolder.newFolder("testListener");
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
        String executionDate = OffsetDateTime.now().format(fmt);

        TransactionItemReaderListener transactionItemReaderListener = new TransactionItemReaderListener();
        transactionItemReaderListener.setExecutionDate(executionDate);
        transactionItemReaderListener.setResolver(new PathMatchingResourcePatternResolver());
        transactionItemReaderListener.setErrorTransactionsLogsPath("file:/"+folder.getAbsolutePath());
        transactionItemReaderListener.onReadError(new FlatFileParseException("Parsing error at line: " +
                1, new Exception(), "input", 1));

        Assert.assertEquals(1,
                FileUtils.listFiles(
                        resolver.getResources("classpath:/test-encrypt/**/testListener")[0].getFile(),
                        new String[]{"csv"},false).size());

    }

    @After
    public void tearDown() {
        tempFolder.delete();
    }

}