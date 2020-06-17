package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

/**
 * Class to be used to map a {@link Transaction} from an {@link InboundTransaction}
 */

@Service
public class TransactionMapper {

    /**
     *
     * @param inboundTransaction
     *              instance of an  {@link InboundTransaction}, to be mapped into a {@link Transaction}
     * @return  {@link Transaction} instance from the input inboundTransaction, normalized and with an hashed PAN
     */
    public Transaction map(InboundTransaction inboundTransaction, Boolean applyHashing) {

        Transaction transaction = null;

        if (inboundTransaction != null) {
            transaction = Transaction.builder().build();
            BeanUtils.copyProperties(inboundTransaction, transaction, "hpan");
            transaction.setHpan(applyHashing ?
                    DigestUtils.sha256Hex(inboundTransaction.getPan()) :
                    inboundTransaction.getPan()
            );
        }

        return transaction;

    }

}
