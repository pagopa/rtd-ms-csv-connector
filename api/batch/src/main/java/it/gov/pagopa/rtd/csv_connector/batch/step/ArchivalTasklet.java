package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

/**
 * implementation of the {@link Tasklet}, in which the execute method contains the logic for processed file archival,
 * based on the status of conclusion for every file processed
 */

@Data
@Slf4j
public class ArchivalTasklet implements Tasklet, InitializingBean {

    private String successPath;
    private String errorPath;
    private String errorDir;
    private String publicKeyDir;
    private Boolean applyEncrypt;


    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    /**
     *
     * @throws IOException
     */
    @Override
    public void afterPropertiesSet() throws IOException {
        Assert.notNull(resolver.getResources("file:" + successPath + "*.pgp"),
                "directory must be set");
        Assert.notNull(resolver.getResources("file:" + errorPath + "*.pgp"),
                "directory must be set");
    }

    /**
     * Method that contains the logic for file archival, based on the exit status of each step obtained from the
     * ChunkContext that contains a filename key in the {@link ExecutionContext}
     * @param stepContribution
     * @param chunkContext
     * @return Status of the tasklet execution
     * @throws IOException
     */
    @SneakyThrows
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws IOException {
        Collection<StepExecution> stepExecutions = chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getStepExecutions();
        for (StepExecution stepExecution : stepExecutions) {
            if (stepExecution.getExecutionContext().containsKey("fileName")) {
                String file = stepExecution.getExecutionContext().getString("fileName");
                String path = null;

                try {
                    path = resolver.getResource(file).getFile().getAbsolutePath();
                } catch (Exception e) {
                    log.error(e.getMessage(),e);
                    path = file.replace("file:/", "");
                }

                try {

                    String archivalPath =
                            BatchStatus.COMPLETED.equals(stepExecution.getStatus()) &&
                                    stepExecution.getFailureExceptions().size() <= 0 ?
                                    successPath : errorPath;

                    file = file.replaceAll("\\\\", "/");
                    String[] filename = file.split("/");

                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

                    archivalPath = resolver.getResources(archivalPath)[0].getFile().getAbsolutePath();

                    File destFile = FileUtils.getFile(archivalPath + "/" + RandomUtils.nextLong() +
                            "_" + OffsetDateTime.now().format(fmt) + "_" + filename[filename.length - 1]);

                    FileUtils.moveFile(FileUtils.getFile(path), destFile);

                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                    throw e;
                }
            }
        }

        if (applyEncrypt) {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources(errorDir.concat("/*.csv"));
            Resource publicKey = resolver.getResource(publicKeyDir);
            for (Resource resource : resources) {
                FileInputStream publicKeyIS = null;
                FileOutputStream outputFOS = null;
                try {
                    resource.getFilename();
                    publicKeyIS = new FileInputStream(publicKey.getFile());
                    outputFOS = new FileOutputStream(resource.getFile().getAbsolutePath().concat(".pgp"));
                    PGPDecryptUtil.encryptFile(outputFOS,
                            resource.getFile().getAbsolutePath(),
                            PGPDecryptUtil.readPublicKey(publicKeyIS),
                            false, true);
                } finally {
                    if (publicKeyIS != null) {
                        publicKeyIS.close();
                    }
                    if (outputFOS != null) {
                        outputFOS.close();
                    }
                    FileUtils.forceDelete(resource.getFile());
                }
            }
        }

        return RepeatStatus.FINISHED;

    }

}
