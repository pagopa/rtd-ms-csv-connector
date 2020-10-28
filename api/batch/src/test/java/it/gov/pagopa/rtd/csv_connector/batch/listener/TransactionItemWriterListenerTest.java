package it.gov.pagopa.rtd.csv_connector.batch.listener;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

public class TransactionItemWriterListenerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(
            new File(getClass().getResource("/test-encrypt").getFile()));

    @SneakyThrows
    @Test
    public void onWriteError_OK() {

        File folder = tempFolder.newFolder("testWriter");
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
        String executionDate = OffsetDateTime.now().format(fmt);

        TransactionItemWriterListener transactionItemWriterListener = new TransactionItemWriterListener();
        transactionItemWriterListener.setExecutionDate(executionDate);
        transactionItemWriterListener.setEnableOnErrorFileLogging(true);
        transactionItemWriterListener.setEnableOnErrorLogging(true);
        transactionItemWriterListener.setResolver(new PathMatchingResourcePatternResolver());
        transactionItemWriterListener.setErrorTransactionsLogsPath("file:/"+folder.getAbsolutePath());
        transactionItemWriterListener.onWriteError(new Exception(), Collections.singletonList(Transaction
                .builder().build()));

        Assert.assertEquals(1,
                FileUtils.listFiles(
                        resolver.getResources("classpath:/test-encrypt/**/testWriter")[0].getFile(),
                        new String[]{"csv"},false).size());

    }

    @After
    public void tearDown() {
        tempFolder.delete();
    }

}