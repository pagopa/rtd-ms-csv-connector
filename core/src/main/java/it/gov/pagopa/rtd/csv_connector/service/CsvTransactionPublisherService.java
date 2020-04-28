package it.gov.pagopa.rtd.csv_connector.service;

import it.gov.pagopa.rtd.csv_connector.model.Transaction;

/**
 * public interface for the CsvTransactionPublisherService
 */
public interface CsvTransactionPublisherService {

    /**
     * Method that has the logic for publishing a Transaction to an outbound channel,
     * calling on the appropriate connector
     * @param transaction
     *              Transaction instance to be published
     */
    public void publishTransactionEvent(Transaction transaction);

}
