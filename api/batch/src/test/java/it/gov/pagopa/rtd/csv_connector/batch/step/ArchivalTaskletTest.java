package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Class for unit testing of the ArchivalTasklet class
 */
public class ArchivalTaskletTest extends BaseTest {

    Path resourceDirectory = Paths.get("src","test","resources");
    String resourcePath = resourceDirectory.toAbsolutePath().toString();
    File successFile;
    File errorFile;
    File overridingSuccessFile;

    @SneakyThrows
    @Before
    public void setUp() {
        String resourcePath = resourceDirectory.toAbsolutePath().toString();
        successFile = new File(resourcePath + "/test-encrypt/success-trx.pgp");
        successFile.createNewFile();
        errorFile = new File(resourcePath + "/test-encrypt/error-trx.pgp");
        errorFile.createNewFile();
    }

    @Test
    public void testArchivalStep() {

        ArchivalTasklet archivalTasklet = new ArchivalTasklet();
        archivalTasklet.setErrorPath("file:/"+resourcePath+"/test-encrypt/error");
        archivalTasklet.setSuccessPath("file:/"+resourcePath+"/test-encrypt/success");

        Assert.assertEquals(0, FileUtils.sizeOfDirectory(
            new File(resourcePath+"/test-encrypt/error")));

        Assert.assertEquals(0, FileUtils.sizeOfDirectory(
            new File(resourcePath+"/test-encrypt/success")));

        StepExecution execution = MetaDataInstanceFactory.createStepExecution();

        List<StepExecution> stepExecutions = new ArrayList<>();

        StepExecution stepExecution1 = MetaDataInstanceFactory.createStepExecution("A",1L);
        stepExecution1.setStatus(BatchStatus.COMPLETED);
        stepExecution1.getExecutionContext().put("fileName", resourcePath + "/test-encrypt/success-trx.pgp");
        stepExecutions.add(stepExecution1);

        StepExecution stepExecution2 = MetaDataInstanceFactory.createStepExecution("B", 1L);
        stepExecution2.setStatus(BatchStatus.FAILED);
        stepExecution2.getExecutionContext().put("fileName", resourcePath + "/test-encrypt/error-trx.pgp");
        stepExecutions.add(stepExecution2);

        StepContext stepContext = new StepContext(execution);
        stepContext.getStepExecution().getJobExecution().addStepExecutions(stepExecutions);
        ChunkContext chunkContext = new ChunkContext(stepContext);

        try {

            archivalTasklet.execute(new StepContribution(execution),chunkContext);

            Assert.assertEquals(1, FileUtils.listFiles(
                    new File(resourcePath+"/test-encrypt/error"),
                            new String[]{"pgp"},false).size());

            Assert.assertEquals(1,
                    FileUtils.listFiles(new File(resourcePath+"/test-encrypt/success"),
                            new String[]{"pgp"},false).size());

            successFile.createNewFile();

            stepExecutions = new ArrayList<>();

            StepExecution stepExecution3 = MetaDataInstanceFactory.createStepExecution("C", 1L);
            stepExecution3.setStatus(BatchStatus.COMPLETED);
            stepExecution3.getExecutionContext().put("fileName", resourcePath + "/test-encrypt/success-trx.pgp");
            stepExecutions.add(stepExecution3);

            stepExecutions.add(stepExecution3);
            execution = MetaDataInstanceFactory.createStepExecution();
            stepContext = new StepContext(execution);
            stepContext.getStepExecution().getJobExecution().addStepExecutions(stepExecutions);
            chunkContext = new ChunkContext(stepContext);

            archivalTasklet.execute(new StepContribution(execution),chunkContext);

            Assert.assertEquals(1,
                    FileUtils.listFiles(new File(resourcePath+"/test-encrypt/success"),
                            new String[]{"pgp"},false).size());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @After
    public void tearDown() {
        FileUtils.listFiles(new File(resourcePath+"/test-encrypt"),
                new String[]{"pgp"},false).forEach(File::delete);
        FileUtils.listFiles(new File(resourcePath+"/test-encrypt/success"),
                new String[]{"pgp"},false).forEach(File::delete);
        FileUtils.listFiles(new File(resourcePath+"/test-encrypt/error"),
                new String[]{"pgp"},false).forEach(File::delete);
    }

}