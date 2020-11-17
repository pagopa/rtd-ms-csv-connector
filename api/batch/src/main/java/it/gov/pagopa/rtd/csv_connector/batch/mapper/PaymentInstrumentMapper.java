package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Class to be used to map a {@link Transaction} from an {@link InboundTransaction}
 */

@Service
@RequiredArgsConstructor
public class PaymentInstrumentMapper {

    /**
     *
     * @param inboundPaymentInstrument
     *              instance of an  {@link InboundPaymentInstrument}, to be mapped into a {@link PaymentInstrumentData}
     * @return  {@link PaymentInstrumentData} instance from the input instance of {@link InboundPaymentInstrument}
     */
    public PaymentInstrumentData map(InboundPaymentInstrument inboundPaymentInstrument, String timestamp) {

        DateTimeFormatter dtf = timestamp != null && !timestamp.isEmpty() ?
                DateTimeFormatter.ofPattern(timestamp).withZone(ZoneId.systemDefault()): null;

        PaymentInstrumentData paymentInstrumentData = PaymentInstrumentData.builder().build();

        if (inboundPaymentInstrument != null) {
            BeanUtils.copyProperties(inboundPaymentInstrument, paymentInstrumentData, "cancellationDate");
        }

        paymentInstrumentData.setCancellationDate(dtf != null ?
                ZonedDateTime.parse(Objects.requireNonNull(inboundPaymentInstrument)
                        .getCancellationDate(), dtf).toOffsetDateTime() :
                OffsetDateTime.parse(Objects.requireNonNull(inboundPaymentInstrument).getCancellationDate()));

        return paymentInstrumentData;

    }

}
