package it.gov.pagopa.rtd.csv_connector.service;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentRestClient;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
class PaymentInstrumentConnectorServiceImpl implements PaymentInstrumentConnectorService {

    private final PaymentInstrumentRestClient paymentInstrumentRestClient;

    public void disablePaymentInstrument(PaymentInstrumentData paymentInstrumentData) {
        paymentInstrumentRestClient.delete(paymentInstrumentData);
    }

}