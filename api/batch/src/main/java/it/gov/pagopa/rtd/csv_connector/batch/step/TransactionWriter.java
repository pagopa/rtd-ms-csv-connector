package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemWriterListener;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Implementation of {@link ItemWriter}, to be used for read/processed Transaction files
 */

@RequiredArgsConstructor
@Slf4j
@Data
@Component
public class TransactionWriter implements ItemWriter<Transaction> {

    private final WriterTrackerService writerTrackerService;
    private final CsvTransactionPublisherService csvTransactionPublisherService;
    private TransactionItemWriterListener transactionItemWriterListener;
    private Integer executorPoolSize;

    /**
     * Implementation of the {@link ItemWriter} write method, used for {@link Transaction} as the processed class
     * @param transactions
     *           list of {@link Transaction} from the process phase of a reader to be sent on an outbound Kafka channel
     * @throws Exception
     */
    @Override
    public void write(List<? extends Transaction> transactions) throws Exception {

        Executor executor = Executors.newFixedThreadPool(executorPoolSize);
        CountDownLatch countDownLatch = new CountDownLatch(transactions.size());
        writerTrackerService.addCountDownLatch(countDownLatch);

        transactions.forEach(transaction -> executor.execute(() -> {
            try {
                csvTransactionPublisherService.publishTransactionEvent(transaction);
                countDownLatch.countDown();
            } catch (Exception e) {
                transactionItemWriterListener.onWriteError(e, transaction);
            }
        }));

    }

}
