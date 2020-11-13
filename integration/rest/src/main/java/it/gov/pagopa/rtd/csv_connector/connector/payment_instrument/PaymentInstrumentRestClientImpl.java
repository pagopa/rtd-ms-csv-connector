package it.gov.pagopa.rtd.csv_connector.connector.payment_instrument;

import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Slf4j
@RequiredArgsConstructor
@Service
public class PaymentInstrumentRestClientImpl implements PaymentInstrumentRestClient {

    @Value("${rest-client.payment_instrument.api.key}")
    private String apiKey;

    private final PaymentInstrumentConnector paymentInstrumentConnector;

    @Override
    public void delete(PaymentInstrumentData paymentInstrumentData) {
        paymentInstrumentConnector.deleteByFiscalCode(
                paymentInstrumentData.getHpan(),
                paymentInstrumentData.getFiscalCode(),
                OffsetDateTime.parse(paymentInstrumentData.getCancellationDate()),
                apiKey);
    }

}
