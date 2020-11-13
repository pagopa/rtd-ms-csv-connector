package it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentConnector;
import lombok.*;

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
    private String cancellationDate;


}
