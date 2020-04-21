package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Slf4j
@Component
public class InboundTransactionItemProcessor implements ItemProcessor<InboundTransaction, Transaction> {

    private final TransactionMapper mapper;

    @Override
    public Transaction process(InboundTransaction inboundTransaction) {

        //TODO: Apply normalizer
        Transaction transaction = mapper.map(inboundTransaction);

        //TODO: Apply Hashing for PAN
        transaction.setHpan(inboundTransaction.getPan());

        return transaction;

    }

}
