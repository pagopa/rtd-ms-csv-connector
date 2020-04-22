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

import java.util.Collection;

@Data
@Slf4j
public class ArchivalTasklet implements Tasklet, InitializingBean {

    private String errorPath;
    private String successPath;



    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(new PathMatchingResourcePatternResolver()
                .getResources("file:" + successPath + "*.pgp"), "directory must be set");
        Assert.notNull(new PathMatchingResourcePatternResolver()
                .getResources("file:" + errorPath + "*.pgp"), "directory must be set");
    }

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
                                    successPath.replace("file:/", "") :
                                    errorPath.replace("file:/", "");

                    String[] filename = file.split("/");
                    FileUtils.moveFile(FileUtils.getFile(path), FileUtils.getFile(archivalPath + "/" +
                            filename[filename.length - 1]));
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
