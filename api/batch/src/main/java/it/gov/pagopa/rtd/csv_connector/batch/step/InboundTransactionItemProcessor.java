package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import javax.validation.*;
import java.util.Set;

/**
 * Implementation of the ItemProcessor interface, used to process instances of InboundTransaction,
 * to be mapped into a normalized version defined as instances of Transaction
 */

@RequiredArgsConstructor
@Slf4j
@Component
public class InboundTransactionItemProcessor implements ItemProcessor<InboundTransaction, Transaction> {

    private final TransactionMapper mapper;

    private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    private static final Validator validator = factory.getValidator();

    /**
     * Validates the input InboundTransaction, and maps it to an instance of Transaction
     * @param inboundTransaction
     *              instance of InboundTransaction from the read phase of the step
     * @return instance of Transaction, mapped from a normalized instance of InboundTransaction
     * @throws ConstraintViolationException
     */
    @Override
    public Transaction process(InboundTransaction inboundTransaction) {

        Set<ConstraintViolation<InboundTransaction>> constraintViolations = validator.validate(inboundTransaction);
        if (constraintViolations.size() > 0) {
            throw new ConstraintViolationException(constraintViolations);
        }

        Transaction transaction = mapper.map(inboundTransaction);

        return transaction;

    }

}
