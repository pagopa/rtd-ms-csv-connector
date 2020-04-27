package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

/**
 * @author Alessio Cialini
 * Class to be used to map a Transaction from an InboundTransaction
 */

@Service
public class TransactionMapper {

    /**
     *
     * @param inboundTransaction
     *              instance of an InboundTransaction, to be mapped into a Tranaction
     * @return Transaction instance from the input inboundTransaction, normalized and with an hashed PAN
     */
    public Transaction map(InboundTransaction inboundTransaction) {

        Transaction transaction = null;

        if (inboundTransaction != null) {
            transaction = Transaction.builder().build();
            BeanUtils.copyProperties(inboundTransaction, transaction, "hpan");
        }

        transaction.setHpan(DigestUtils.sha256Hex(inboundTransaction.getPan()));

        return transaction;

    }

}
