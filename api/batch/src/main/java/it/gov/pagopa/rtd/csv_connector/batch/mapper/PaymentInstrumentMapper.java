package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

/**
 * Class to be used to map a {@link Transaction} from an {@link InboundTransaction}
 */

@Service
public class PaymentInstrumentMapper {

    /**
     *
     * @param inboundPaymentInstrument
     *              instance of an  {@link InboundPaymentInstrument}, to be mapped into a {@link PaymentInstrumentData}
     * @return  {@link PaymentInstrumentData} instance from the input instance of {@link InboundPaymentInstrument}
     */
    public PaymentInstrumentData map(InboundPaymentInstrument inboundPaymentInstrument) {

        PaymentInstrumentData paymentInstrumentData = PaymentInstrumentData.builder().build();

        if (inboundPaymentInstrument != null) {
            BeanUtils.copyProperties(inboundPaymentInstrument, paymentInstrumentData);
        }

        return paymentInstrumentData;

    }

}
