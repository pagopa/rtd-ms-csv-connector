package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import it.gov.pagopa.rtd.csv_connector.service.CsvTransactionPublisherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Alessio Cialini
 * Implementation of the itemReader interface, to be used for read/processed Transaction files
 */

@RequiredArgsConstructor
@Slf4j
@Component
public class TransactionWriter implements ItemWriter<Transaction> {

    private final CsvTransactionPublisherService csvTransactionPublisherService;

    /**
     * Implementation of the itemReader write method, used for Transaction as the processed class
     * @param transactions
     *              list of Transaction from the process phase of a reader to be sent on an outbound Kafka channel
     * @throws Exception
     */
    @Override
    public void write(List<? extends Transaction> transactions) throws Exception {
        transactions.forEach(csvTransactionPublisherService::publishTransactionEvent);
    }

}
