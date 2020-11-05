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

//    @Test
//    public void processValidInboundTransaction_NoHashing() {
//
//        try {
//            InboundTransaction inboundTransaction = getInboundTransaction();
//            this.inboundTransactionItemProcessor.setApplyHashing(false);
//            Transaction transaction = inboundTransactionItemProcessor.
//                    process(inboundTransaction);
//            Assert.assertNotNull(transaction);
//            Assert.assertEquals(transaction.getHpan(), inboundTransaction.getPan());
//            Assert.assertEquals(BigDecimal.valueOf(10.50).setScale(2),transaction.getAmount());
//            Mockito.verify(mapperSpy).map(Mockito.eq(inboundTransaction), Mockito.eq(false));
//        } catch (Exception e) {
//            e.printStackTrace();
//            Assert.fail();
//        }
//
//    }
//
//    @Test
//    public void process_OK_validForReturn() {
//        try {
//
//            InboundTransaction inboundTransaction = getInboundTransaction();
//            inboundTransaction.setOperationType("01");
//            inboundTransaction.setIdTrxIssuer("");
//            this.inboundTransactionItemProcessor.setApplyHashing(false);
//
//            Transaction transaction = inboundTransactionItemProcessor.process(inboundTransaction);
//            Assert.assertNotNull(transaction);
//            Assert.assertEquals(transaction.getHpan(), inboundTransaction.getPan());
//            Mockito.verify(mapperSpy).map(Mockito.eq(inboundTransaction), Mockito.eq(false));
//            inboundTransaction = getInboundTransaction();
//            inboundTransaction.setOperationType("01");
//            inboundTransaction.setIdTrxIssuer(null);
//            transaction = inboundTransactionItemProcessor.process(inboundTransaction);
//            Assert.assertNotNull(transaction);
//            Assert.assertEquals(inboundTransaction.getPan(),transaction.getHpan());
//            Assert.assertEquals(BigDecimal.valueOf(10.50).setScale(2),transaction.getAmount());
//            Mockito.verify(mapperSpy, Mockito.times(2)).map(Mockito.eq(inboundTransaction), Mockito.eq(false));
//
//        } catch (Exception e) {
//            Assert.fail();
//            e.printStackTrace();
//        }
//    }

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