package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import it.gov.pagopa.rtd.csv_connector.service.CsvTransactionPublisherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.InitializingBean;

@RequiredArgsConstructor
@Slf4j
public class PGPItemProcessor implements ItemProcessor<byte[], Transaction>, InitializingBean {

    private final CsvTransactionPublisherService csvTransactionPublisherService;

    @Override
    public void afterPropertiesSet() throws Exception {

    }

    @Override
    public Transaction process(byte[] inputTransactionData) throws Exception {

        //TODO: Complete processor logic

        Transaction transaction = new Transaction();
        csvTransactionPublisherService.publishInvoiceTransactionEvent(transaction);
        return transaction;
    }
}
