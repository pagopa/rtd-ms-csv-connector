package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.PaymentInstrumentMapper;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import it.gov.pagopa.rtd.csv_connector.service.PaymentInstrumentConnectorService;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.util.Collections;

public class PaymentInstrumentWriterTest extends BaseTest {

    @Mock
    private PaymentInstrumentConnectorService paymentInstrumentConnectorServiceMock;

    @Spy
    private PaymentInstrumentMapper paymentInstrumentMapper;

    private PaymentInstrumentWriter paymentInstrumentWriter;


    @Before
    public void init() {
        Mockito.reset(paymentInstrumentConnectorServiceMock, paymentInstrumentMapper);
        BDDMockito.doNothing().when(paymentInstrumentConnectorServiceMock).disablePaymentInstrument(Mockito.anyList());
        paymentInstrumentWriter = new PaymentInstrumentWriter(
                paymentInstrumentConnectorServiceMock, paymentInstrumentMapper);
        paymentInstrumentWriter.setTimestampParser("MM/dd/yyyy HH:mm:ss");
    }

    @SneakyThrows
    @Test
    public void writeTest() {
        paymentInstrumentWriter.write(Collections.singletonList(getInboundPaymentInstrument()));
    }

    public InboundPaymentInstrument getInboundPaymentInstrument() {
        return InboundPaymentInstrument.builder()
                .fiscalCode("13131")
                .hpan("pan1")
                .cancellationDate("03/20/2020 10:50:33")
                .filename("test.csv")
                .lineNumber(1)
                .build();
    }

}