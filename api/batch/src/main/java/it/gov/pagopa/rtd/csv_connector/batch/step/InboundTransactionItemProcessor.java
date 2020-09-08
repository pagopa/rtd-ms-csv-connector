package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import lombok.Data;
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
@Data
@Component
public class InboundTransactionItemProcessor implements ItemProcessor<InboundTransaction, Transaction> {

    private final TransactionMapper mapper;
    private Boolean applyHashing;

    private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    private static final Validator validator = factory.getValidator();

    /**
     * Validates the input {@link InboundTransaction}, and maps it to an instance of Transaction
     * @param inboundTransaction
     *              instance of {@link InboundTransaction} from the read phase of the step
     * @return instance of  {@link Transaction}, mapped from a normalized instance of {@link InboundTransaction}
     * @throws ConstraintViolationException
     */
    @Override
    public Transaction process(InboundTransaction inboundTransaction) {

        Set<ConstraintViolation<InboundTransaction>> constraintViolations = validator.validate(inboundTransaction);
        if (constraintViolations.size() > 0) {
            throw new ConstraintViolationException(constraintViolations);
        }

        Transaction transaction = mapper.map(inboundTransaction, applyHashing);

        return transaction;

    }

}
