package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.core.interceptors.BaseContextHolder;
import eu.sia.meda.core.model.ApplicationContext;
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

    private static final String BATCH_CSV_CONNECTOR_NAME = "rtd-ms-csv-connector";

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
     *
     * @param inboundTransactions list of {@link Transaction} from the process phase of a reader to be sent on an outbound Kafka channel
     * @throws Exception
     */
    @Override
    public void write(List<? extends InboundTransaction> inboundTransactions) throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(inboundTransactions.size());
        Integer trackerSize = writerTrackerService.getCountDownLatches().size();

        ApplicationContext applicationContext = BaseContextHolder.getApplicationContext();
        applicationContext.setUserId(BATCH_CSV_CONNECTOR_NAME);
        if (!inboundTransactions.isEmpty()) {
            applicationContext.setRequestId(inboundTransactions.get(0).getFilename().substring(inboundTransactions.get(0).getFilename().lastIndexOf('/') + 1));
        }

        inboundTransactions.forEach(inboundTransaction -> executor.execute(() -> {
            try {
                applicationContext.setRequestId(String.format("%s:%d",
                        applicationContext.getRequestId(),
                        inboundTransaction.getLineNumber()));
                BaseContextHolder.forceSetApplicationContext(applicationContext);
                Transaction transaction = mapper.map(inboundTransaction, applyHashing);
                log.info("test");
                csvTransactionPublisherService.publishTransactionEvent(transaction);
            } catch (Exception e) {
                transactionItemWriterListener.onWriteError(e, inboundTransaction);
            }
            countDownLatch.countDown();
        }));

        writerTrackerService.addCountDownLatch(countDownLatch, enableCheckpointFrequency, checkpointFrequency);

    }

}
