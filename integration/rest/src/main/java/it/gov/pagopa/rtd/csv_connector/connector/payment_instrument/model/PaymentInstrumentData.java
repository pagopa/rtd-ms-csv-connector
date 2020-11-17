package it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentConnector;
import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import java.time.OffsetDateTime;

/**
 * Resource model for the data recovered through {@link PaymentInstrumentConnector}
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = "hpan", callSuper = false)
public class PaymentInstrumentData {

    private String hpan;
    private String fiscalCode;
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    private OffsetDateTime cancellationDate;

}
