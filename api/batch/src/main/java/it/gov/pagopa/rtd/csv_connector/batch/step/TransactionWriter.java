package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import it.gov.pagopa.rtd.csv_connector.service.CsvTransactionPublisherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@RequiredArgsConstructor
@Slf4j
@Component
public class TransactionWriter implements ItemWriter<Transaction> {

    private final CsvTransactionPublisherService csvTransactionPublisherService;

    @Override
    public void write(List<? extends Transaction> transactions) throws Exception {
        transactions.forEach(csvTransactionPublisherService::publishInvoiceTransactionEvent);
    }

}
