package it.gov.pagopa.rtd.csv_connector.service;

import eu.sia.meda.event.transformer.SimpleEventRequestTransformer;
import eu.sia.meda.event.transformer.SimpleEventResponseTransformer;
import it.gov.pagopa.rtd.csv_connector.CsvTransactionPublisherConnector;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Implementation of {@link CsvTransactionPublisherService}
 */
@Service
@Slf4j
class CsvTransactionPublisherServiceImpl implements CsvTransactionPublisherService {

    private final CsvTransactionPublisherConnector csvTransactionPublisherConnector;
    private final SimpleEventRequestTransformer<Transaction> simpleEventRequestTransformer;
    private final SimpleEventResponseTransformer simpleEventResponseTransformer;

    @Autowired
    public CsvTransactionPublisherServiceImpl(CsvTransactionPublisherConnector csvTransactionPublisherConnector,
                                              SimpleEventRequestTransformer<Transaction> simpleEventRequestTransformer,
                                              SimpleEventResponseTransformer simpleEventResponseTransformer) {
        this.csvTransactionPublisherConnector = csvTransactionPublisherConnector;
        this.simpleEventRequestTransformer = simpleEventRequestTransformer;
        this.simpleEventResponseTransformer = simpleEventResponseTransformer;
    }

    /**
     * Method that has the logic for publishing a {@link Transaction} to an outbound channel,
     * calling on the appropriate connector
     * @param transaction
     *              {@link Transaction} instance to be published
     */
    @Override
    public void publishTransactionEvent(Transaction transaction) {
        csvTransactionPublisherConnector.doCall(
                transaction, simpleEventRequestTransformer, simpleEventResponseTransformer);
    }

}
