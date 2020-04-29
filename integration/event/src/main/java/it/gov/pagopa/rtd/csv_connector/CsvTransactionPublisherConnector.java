package it.gov.pagopa.rtd.csv_connector;

import eu.sia.meda.event.BaseEventConnector;
import eu.sia.meda.event.transformer.IEventRequestTransformer;
import eu.sia.meda.event.transformer.IEventResponseTransformer;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import org.springframework.stereotype.Service;

/**
 * Class extending the MEDA {@link BaseEventConnector}, is responsible for calling
 * a Kafka outbound channel with messagescontaining a json mapped on the Transaction class
 */

@Service
public class CsvTransactionPublisherConnector extends BaseEventConnector<Transaction, Boolean, Transaction, Void> {

    /**
     *
     * @param transaction
                Transaction instance to be used as message content
     * @param requestTransformer
                Trannsformer for the request data
     * @param responseTransformer
                Transformer for the call response
     * @param args
                Additional args to be used in the call
     * @return Exit status for the call
     */
    public Boolean doCall(
            Transaction transaction, IEventRequestTransformer<Transaction,
            Transaction> requestTransformer,
            IEventResponseTransformer<Void, Boolean> responseTransformer,
            Object... args) {
        return this.call(transaction, requestTransformer, responseTransformer, args);
    }

}
