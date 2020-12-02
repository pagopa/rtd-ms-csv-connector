package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemWriterListener;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import it.gov.pagopa.rtd.csv_connector.service.CsvTransactionPublisherService;
import it.gov.pagopa.rtd.csv_connector.service.WriterTrackerService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

/**
 * Implementation of {@link ItemWriter}, to be used for read/processed Transaction files
 */

@RequiredArgsConstructor
@Slf4j
@Data
@Component
public class TransactionWriter implements ItemWriter<InboundTransaction> {

    private final WriterTrackerService writerTrackerService;
    private final CsvTransactionPublisherService csvTransactionPublisherService;
    private TransactionItemWriterListener transactionItemWriterListener;
    private Executor executor;
    private final TransactionMapper mapper;
    private Boolean applyHashing;
    private Boolean enableCheckpointFrequency;
    private Integer checkpointFrequency;

    /**
     * Implementation of the {@link ItemWriter} write method, used for {@link Transaction} as the processed class
     * @param inboundTransactions
     *           list of {@link Transaction} from the process phase of a reader to be sent on an outbound Kafka channel
     * @throws Exception
     */
    @Override
    public void write(List<? extends InboundTransaction> inboundTransactions) throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(inboundTransactions.size());
        writerTrackerService.addCountDownLatch(countDownLatch);
        Integer trackerSize = writerTrackerService.getCountDownLatches().size();

        inboundTransactions.forEach(inboundTransaction -> executor.execute(() -> {
            try {
                Transaction transaction = mapper.map(inboundTransaction, applyHashing);
                csvTransactionPublisherService.publishTransactionEvent(transaction);
            } catch (Exception e) {
                transactionItemWriterListener.onWriteError(e, inboundTransaction);
            }
            countDownLatch.countDown();
        }));

        if (enableCheckpointFrequency && trackerSize % checkpointFrequency == 0) {
            countDownLatch.await();
        }

    }

}
