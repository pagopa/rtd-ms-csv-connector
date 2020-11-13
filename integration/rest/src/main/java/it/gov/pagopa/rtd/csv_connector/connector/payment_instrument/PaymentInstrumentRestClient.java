package it.gov.pagopa.rtd.csv_connector.connector.payment_instrument;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;

public interface PaymentInstrumentRestClient {

    void delete(PaymentInstrumentData paymentInstrumentData);

}
