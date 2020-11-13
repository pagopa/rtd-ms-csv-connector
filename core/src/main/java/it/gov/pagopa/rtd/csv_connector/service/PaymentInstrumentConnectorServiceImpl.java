package it.gov.pagopa.rtd.csv_connector.service;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentRestClient;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Service
public class PaymentInstrumentConnectorServiceImpl implements PaymentInstrumentConnectorService {

    private final PaymentInstrumentRestClient paymentInstrumentRestClient;

    public void disablePaymentInstrument(List<PaymentInstrumentData> paymentInstrumentRequests) {
        paymentInstrumentRequests.forEach(paymentInstrumentRestClient::delete);
    }

}