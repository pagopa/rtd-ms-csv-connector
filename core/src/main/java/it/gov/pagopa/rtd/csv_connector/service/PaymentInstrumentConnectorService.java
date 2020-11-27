package it.gov.pagopa.rtd.csv_connector.service;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;

public interface PaymentInstrumentConnectorService {

    void disablePaymentInstrument(PaymentInstrumentData paymentInstrumentRequest);

}
