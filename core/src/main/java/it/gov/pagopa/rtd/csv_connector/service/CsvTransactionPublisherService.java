package it.gov.pagopa.rtd.csv_connector.service;

import it.gov.pagopa.rtd.csv_connector.model.Transaction;

public interface CsvTransactionPublisherService {

    public void publishTransactionEvent(Transaction transaction);

}
