package it.gov.pagopa.rtd.csv_connector.service;

import it.gov.pagopa.rtd.csv_connector.model.Transaction;

/**
 * Interface for the event publisher service, responsible for handling the transactions
 * to be passed through the connector for the outbound channel
 * @see CsvTransactionPublisherServiceImpl
 */
public interface CsvTransactionPublisherService {

    /**
     * Method that has the logic for publishing a {@link Transaction} to an outbound channel,
     * calling on the appropriate connector
     * @param transaction
     *            {@link Transaction} instance to be published
     */
    void publishTransactionEvent(Transaction transaction);

}
