package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.ConstraintViolationException;

public class InboundPaymentInstrumentItemProcessorTest extends BaseTest {

    private InboundPaymentInstrumentItemProcessor inboundPaymentInstrumentItemProcessor;

    @Before
    public void initTest() {
        this.inboundPaymentInstrumentItemProcessor = new InboundPaymentInstrumentItemProcessor();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void processValidInboundPaymentInstrument() {

        try {
            InboundPaymentInstrument inboundPaymentInstrument = getInboundPaymentInstrument();
            InboundPaymentInstrument paymentInstrument = inboundPaymentInstrumentItemProcessor.
                    process(inboundPaymentInstrument);
            Assert.assertNotNull(paymentInstrument);
            Assert.assertEquals(paymentInstrument, paymentInstrument);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void processInvalidInboundTransaction() {

        InboundPaymentInstrument inboundPaymentInstrument = getInboundPaymentInstrument();
        inboundPaymentInstrument.setFiscalCode(null);

        exceptionRule.expect(ConstraintViolationException.class);
        inboundPaymentInstrumentItemProcessor.process(inboundPaymentInstrument);

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