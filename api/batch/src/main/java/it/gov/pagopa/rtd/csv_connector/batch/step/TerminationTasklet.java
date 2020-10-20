package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.service.WriterTrackerService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Data
@Slf4j
public class TerminationTasklet implements Tasklet, InitializingBean {

    private final WriterTrackerService writerTrackerService;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        List<CountDownLatch> countDownLatchList = writerTrackerService.getCountDownLatches();

        if (!countDownLatchList.isEmpty()) {

            countDownLatchList.get(countDownLatchList.size()-1).await();

            for (CountDownLatch countDownLatch : countDownLatchList) {
                countDownLatch.await();
            }
        }

        return RepeatStatus.FINISHED;
    }

    @Override
    public void afterPropertiesSet() throws Exception {}
}
