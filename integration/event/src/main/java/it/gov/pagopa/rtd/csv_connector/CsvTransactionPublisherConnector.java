package it.gov.pagopa.rtd.csv_connector;

import eu.sia.meda.event.BaseEventConnector;
import eu.sia.meda.event.transformer.IEventRequestTransformer;
import eu.sia.meda.event.transformer.IEventResponseTransformer;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import org.springframework.stereotype.Service;

@Service
public class CsvTransactionPublisherConnector extends BaseEventConnector<Transaction, Boolean, Transaction, Void> {

    public Boolean doCall(
            Transaction transaction, IEventRequestTransformer<Transaction,
            Transaction> requestTransformer,
            IEventResponseTransformer<Void, Boolean> responseTransformer,
            Object... args) {
        return this.call(transaction, requestTransformer, responseTransformer, args);
    }

}