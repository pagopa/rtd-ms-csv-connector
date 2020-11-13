package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.Spy;

import javax.validation.ConstraintViolationException;
import java.math.BigDecimal;

/**
 * Class for unit testing of the InboundTransactionItemProcessor class
 */
public class InboundTransactionItemProcessorTest extends BaseTest {



    private InboundTransactionItemProcessor inboundTransactionItemProcessor;

    @Before
    public void initTest() {
        this.inboundTransactionItemProcessor = new InboundTransactionItemProcessor();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void processValidInboundTransaction() {

        try {
            InboundTransaction inboundTransaction = getInboundTransaction();
            InboundTransaction transaction = inboundTransactionItemProcessor.
                    process(inboundTransaction);
            Assert.assertNotNull(transaction);
            Assert.assertEquals(transaction.getPan(), inboundTransaction.getPan());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void processInvalidInboundTransaction() {

        InboundTransaction inboundTransaction = getInboundTransaction();
        inboundTransaction.setCircuitType("000");
        inboundTransaction.setAcquirerCode(null);

        exceptionRule.expect(ConstraintViolationException.class);
        inboundTransactionItemProcessor.process(inboundTransaction);

    }

    protected InboundTransaction getInboundTransaction() {
        return InboundTransaction.builder()
                .idTrxAcquirer("1")
                .acquirerCode("001")
                .trxDate("2020-04-09T16:22:45.304Z")
                .amount(1050L)
                .operationType("00")
                .pan("hpan")
                .merchantId("0")
                .circuitType("00")
                .mcc("813")
                .idTrxIssuer("0")
                .amountCurrency("833")
                .correlationId("1")
                .acquirerId("0")
                .terminalId("0")
                .bin("000000")
                .build();
    }

}