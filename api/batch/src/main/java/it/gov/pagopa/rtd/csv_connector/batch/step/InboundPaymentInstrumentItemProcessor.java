package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import javax.validation.*;
import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of the ItemProcessor interface, used to process instances of InboundPaymentInstrument,
 * to be mapped into a normalized version defined as instances of InboundPaymentInstrument
 */

@RequiredArgsConstructor
@Slf4j
@Data
@Component
public class InboundPaymentInstrumentItemProcessor
        implements ItemProcessor<InboundPaymentInstrument, InboundPaymentInstrument> {

    private static final ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    private static final Validator validator = factory.getValidator();
    private Set<InboundPaymentInstrument> inboundPaymentInstruments = new HashSet<>();

    /**
     * Validates the input {@link InboundPaymentInstrument}, and maps it to an instance of Transaction
     * @param inboundPaymentInstrument
     *              instance of {@link InboundPaymentInstrument} from the read phase of the step
     * @return instance of {@link InboundPaymentInstrument}, if valid
     * @throws ConstraintViolationException
     */
    @Override
    public InboundPaymentInstrument process(@NotNull InboundPaymentInstrument inboundPaymentInstrument) {

        Set<ConstraintViolation<InboundPaymentInstrument>> constraintViolations =
                validator.validate(inboundPaymentInstrument);
        if (constraintViolations.size() > 0) {
            throw new ConstraintViolationException(constraintViolations);
        }

        if(inboundPaymentInstruments.contains(inboundPaymentInstrument)) {
            return null;
        }

        inboundPaymentInstruments.add(inboundPaymentInstrument);

        return inboundPaymentInstrument;

    }

}
