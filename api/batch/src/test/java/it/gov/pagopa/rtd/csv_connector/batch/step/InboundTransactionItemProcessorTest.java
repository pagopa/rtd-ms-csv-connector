package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
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
import java.time.OffsetDateTime;

public class InboundTransactionItemProcessorTest extends BaseTest {

    @Spy
    private TransactionMapper mapperSpy;

    private InboundTransactionItemProcessor inboundTransactionItemProcessor;

    @Before
    public void initTest() {
        Mockito.reset(mapperSpy);
        this.inboundTransactionItemProcessor = new InboundTransactionItemProcessor(mapperSpy);
    }

    @Test
    public void processValidInboundTransaction() {

        try {
            InboundTransaction inboundTransaction = getInboundTransaction();
            Transaction transaction = inboundTransactionItemProcessor.
                    process(inboundTransaction);
            Assert.assertNotNull(transaction);
            Assert.assertEquals(transaction.getHpan(), DigestUtils.sha256Hex(inboundTransaction.getPan()));
            Mockito.verify(mapperSpy).map(Mockito.eq(inboundTransaction));
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

        Mockito.verifyZeroInteractions(mapperSpy);

    }

    protected InboundTransaction getInboundTransaction() {
        return InboundTransaction.builder()
                .idTrxAcquirer(1)
                .acquirerCode("001")
                .trxDate(OffsetDateTime.parse("2020-04-09T16:22:45.304Z"))
                .amount(BigDecimal.valueOf(1313.13))
                .operationType("00")
                .pan("hpan")
                .merchantId("0")
                .circuitType("00")
                .mcc("813")
                .idTrxIssuer(0)
                .amountCurrency("833")
                .correlationId("1")
                .acquirerId("0")
                .build();
    }

}