package it.gov.pagopa.rtd.csv_connector.service;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;

import java.util.List;

public interface PaymentInstrumentConnectorService {

    void disablePaymentInstrument(List<PaymentInstrumentData> paymentInstrumentRequest);

}