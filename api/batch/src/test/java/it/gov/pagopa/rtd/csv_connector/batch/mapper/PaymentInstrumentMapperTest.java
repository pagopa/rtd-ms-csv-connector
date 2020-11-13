package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PaymentInstrumentMapperTest {

    private PaymentInstrumentMapper paymentInstrumentMapper;

    @Before
    public void setUp() {
        paymentInstrumentMapper = new PaymentInstrumentMapper();
    }

    @SneakyThrows
    @Test
    public void testMapper() {
        PaymentInstrumentData paymentInstrumentData =
                paymentInstrumentMapper.map(getInboundPaymentInstrument());
        Assert.assertNotNull(paymentInstrumentData);
        Assert.assertEquals(paymentInstrumentData, getPaymentInstrumentData());
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

    public PaymentInstrumentData getPaymentInstrumentData() {
        return PaymentInstrumentData.builder()
                .fiscalCode("13131")
                .hpan("pan1")
                .cancellationDate("03/20/2020 10:50:33")
                .build();
    }


}
