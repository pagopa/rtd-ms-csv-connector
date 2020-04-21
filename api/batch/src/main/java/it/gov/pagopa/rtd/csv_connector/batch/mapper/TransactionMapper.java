package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

@Service
public class TransactionMapper {

    public Transaction map(InboundTransaction inboundTransaction) {

        Transaction transaction = null;

        if (inboundTransaction != null) {
            transaction = Transaction.builder().build();
            BeanUtils.copyProperties(inboundTransaction, transaction, "hpan");
        }

        return transaction;
    }

}
