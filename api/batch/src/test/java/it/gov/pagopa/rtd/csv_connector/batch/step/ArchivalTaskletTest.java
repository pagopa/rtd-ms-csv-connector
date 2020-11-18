package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for unit testing of the ArchivalTasklet class
 */
public class ArchivalTaskletTest extends BaseTest {


    File successFile;
    File errorFile;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(
            new File(getClass().getResource("/test-encrypt").getFile()));

    @SneakyThrows
    @Before
    public void setUp() {
        tempFolder.newFolder("success");
        tempFolder.newFolder("error");
        successFile = tempFolder.newFile("success-trx.pgp");
        errorFile =  tempFolder.newFile("error-trx.pgp");
    }

    @Test
    public void testArchivalStep() {

        try {

            ArchivalTasklet archivalTasklet = new ArchivalTasklet();
            archivalTasklet.setErrorPath("classpath:/test-encrypt/**/error");
            archivalTasklet.setSuccessPath("classpath:/test-encrypt/**/success");
            archivalTasklet.setApplyEncrypt(false);

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());
            StepExecution execution = MetaDataInstanceFactory.createStepExecution();

            List<StepExecution> stepExecutions = new ArrayList<>();

            StepExecution stepExecution1 = MetaDataInstanceFactory.createStepExecution("A",1L);
            stepExecution1.setStatus(BatchStatus.COMPLETED);
            stepExecution1.getExecutionContext().put("fileName", successFile.getAbsolutePath());
            stepExecutions.add(stepExecution1);

            StepExecution stepExecution2 = MetaDataInstanceFactory.createStepExecution("B", 1L);
            stepExecution2.setStatus(BatchStatus.FAILED);
            stepExecution2.getExecutionContext().put("fileName", errorFile.getAbsolutePath());
            stepExecutions.add(stepExecution2);

            StepContext stepContext = new StepContext(execution);
            stepContext.getStepExecution().getJobExecution().addStepExecutions(stepExecutions);
            ChunkContext chunkContext = new ChunkContext(stepContext);

            archivalTasklet.execute(new StepContribution(execution),chunkContext);

            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

            successFile.createNewFile();

            stepExecutions = new ArrayList<>();

            StepExecution stepExecution3 = MetaDataInstanceFactory.createStepExecution("C", 1L);
            stepExecution3.setStatus(BatchStatus.COMPLETED);
            stepExecution3.getExecutionContext().put("fileName",successFile.getAbsolutePath());
            stepExecutions.add(stepExecution3);

            stepExecutions.add(stepExecution3);
            execution = MetaDataInstanceFactory.createStepExecution();
            stepContext = new StepContext(execution);
            stepContext.getStepExecution().getJobExecution().addStepExecutions(stepExecutions);
            chunkContext = new ChunkContext(stepContext);

            archivalTasklet.execute(new StepContribution(execution),chunkContext);

            Assert.assertEquals(2,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @After
    public void tearDown() {
        tempFolder.delete();
    }

}