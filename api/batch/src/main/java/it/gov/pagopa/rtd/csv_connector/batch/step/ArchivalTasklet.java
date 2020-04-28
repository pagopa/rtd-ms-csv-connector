package it.gov.pagopa.rtd.csv_connector.batch.step;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.Assert;

import java.io.File;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;

/**
 * implementation of the Tasklet interface, in which the execute method contains the logic for processed file archival,
 * based on the status of conclusion for every file processed
 */

@Data
@Slf4j
public class ArchivalTasklet implements Tasklet, InitializingBean {

    private String successPath;
    private String errorPath;

    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    /**
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(resolver.getResources("file:" + successPath + "*.pgp"),
                "directory must be set");
        Assert.notNull(resolver.getResources("file:" + errorPath + "*.pgp"),
                "directory must be set");
    }

    /**
     * Method that contains the logic for file archival, based on the exit status of each step obtained from the
     * ChunkContext that contains a filename key in the ExecutionContext
     * @param stepContribution
     * @param chunkContext
     * @return Status of the tasklet execution
     * @throws Exception
     */
    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Collection<StepExecution> stepExecutions = chunkContext.getStepContext().getStepExecution().getJobExecution()
                .getStepExecutions();
        for (StepExecution stepExecution : stepExecutions) {
            if (stepExecution.getExecutionContext().containsKey("fileName")) {
                String file = stepExecution.getExecutionContext().getString("fileName");
                String path = file.replace("file:/", "");

                try {

                    String archivalPath =
                            BatchStatus.COMPLETED.equals(stepExecution.getStatus()) &&
                                    stepExecution.getFailureExceptions().size() <= 0 ?
                                    successPath : errorPath;

                    String[] filename = file.split("/");

                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

                    archivalPath = resolver.getResources(archivalPath)[0].getFile().getAbsolutePath();

                    File destFile = FileUtils.getFile(archivalPath + "/" +
                            OffsetDateTime.now().format(fmt) + "_" + filename[filename.length - 1]);

                    FileUtils.moveFile(FileUtils.getFile(path), destFile);

                } catch (Exception e) {
                    if (log.isErrorEnabled()) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }

        return RepeatStatus.FINISHED;

    }

}
