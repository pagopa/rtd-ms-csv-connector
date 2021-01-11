package it.gov.pagopa.rtd.csv_connector.batch.listener;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.service.WriterTrackerService;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class TransactionReaderStepListenerTest extends BaseTest {

    File successFile;

    @Mock
    private WriterTrackerService writerTrackerService;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(
            new File(getClass().getResource("/test-encrypt").getFile()));

    @SneakyThrows
    @Before
    public void setUp() {
        tempFolder.newFolder("success");
        successFile = tempFolder.newFile("success-trx.pgp");
        BDDMockito.doReturn(Collections.singletonList(new CountDownLatch(0)))
                .when(writerTrackerService).getFileCountDownLatches(Mockito.eq(successFile.getAbsolutePath()));
    }

    @SneakyThrows
    @Test
    public void afterStepWithSkips() {

        StepExecution stepExecution = new StepExecution("test-step", new JobExecution(1L));
        stepExecution.setStatus(BatchStatus.COMPLETED);
        stepExecution.setProcessSkipCount(1);
        stepExecution.getExecutionContext().put("fileName",successFile.getAbsolutePath());

        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Assert.assertEquals(0,
                FileUtils.listFiles(
                        resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                        new String[]{"pgp"},false).size());

        TransactionReaderStepListener transactionReaderStepListener = new TransactionReaderStepListener();
        transactionReaderStepListener.setErrorPath("classpath:/test-encrypt/**/error");
        transactionReaderStepListener.setSuccessPath("classpath:/test-encrypt/**/success");
        transactionReaderStepListener.setWriterTrackerService(writerTrackerService);
        transactionReaderStepListener.setApplyEncrypt(false);
        ExitStatus status = transactionReaderStepListener.afterStep(stepExecution);
        Assert.assertEquals(new ExitStatus("COMPLETED WITH SKIPS"), status);

        Assert.assertEquals(1,
                FileUtils.listFiles(
                        resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                        new String[]{"pgp"},false).size());

    }

}