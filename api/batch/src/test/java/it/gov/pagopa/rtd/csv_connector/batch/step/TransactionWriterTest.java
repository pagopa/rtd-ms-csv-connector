package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemWriterListener;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.TransactionMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import it.gov.pagopa.rtd.csv_connector.service.CsvTransactionPublisherService;
import it.gov.pagopa.rtd.csv_connector.service.WriterTrackerService;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.concurrent.Executors;

/**
 * Class for unit testing of the TransactionWriter class
 */
public class TransactionWriterTest extends BaseTest {

    @Mock
    private CsvTransactionPublisherService csvTransactionPublisherServiceMock;

    @Mock
    private WriterTrackerService writerTrackerServiceMock;

    @Mock
    private TransactionItemWriterListener transactionItemWriterListenerMock;

    private TransactionWriter transactionWriter;

    @Spy
    private TransactionMapper mapperSpy;


    @Before
    public void initTest() {
        Mockito.reset(csvTransactionPublisherServiceMock, mapperSpy);
        transactionWriter = new TransactionWriter(
                writerTrackerServiceMock, csvTransactionPublisherServiceMock, mapperSpy);
        transactionWriter.setTransactionItemWriterListener(transactionItemWriterListenerMock);
        transactionWriter.setExecutor(Executors.newSingleThreadExecutor());
        transactionWriter.setApplyHashing(true);
        transactionWriter.setCheckpointFrequency(3);
        transactionWriter.setEnableCheckpointFrequency(true);
        BDDMockito.doNothing().when(csvTransactionPublisherServiceMock)
                .publishTransactionEvent(Mockito.any(Transaction.class));
        BDDMockito.doNothing().when(writerTrackerServiceMock)
                .addCountDownLatch(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @SneakyThrows
    @Test
    public void testWriterNullList() {
        exceptionRule.expect(NullPointerException.class);
        transactionWriter.write(null);
        BDDMockito.verifyZeroInteractions(csvTransactionPublisherServiceMock);
    }

    @Test
    public void testWriterEmptyList() {
        try {
            transactionWriter.write(Collections.emptyList());
            BDDMockito.verifyZeroInteractions(csvTransactionPublisherServiceMock);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testWriterMonoList() {
        try {
            transactionWriter.write(Collections.singletonList(getInboundTransaction()));
            Thread.sleep(1000);
            BDDMockito.verify(csvTransactionPublisherServiceMock, Mockito.times(1))
                    .publishTransactionEvent(Mockito.eq(getTransaction()));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testWriterMultiList() {
        try {
            transactionWriter.write(Collections.nCopies(5,getInboundTransaction()));
            Thread.sleep(1000);
            BDDMockito.verify(csvTransactionPublisherServiceMock, Mockito.times(5))
                    .publishTransactionEvent(Mockito.eq(getTransaction()));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
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
                .bin("0000")
                .par("par")
                .filename("filename")
                .lineNumber(1)
                .build();
    }

    protected Transaction getTransaction() {
        return Transaction.builder()
                .idTrxAcquirer("1")
                .acquirerCode("001")
                .trxDate("2020-04-09T16:22:45.304Z")
                .amount(BigDecimal.valueOf(10.50).setScale(2))
                .operationType("00")
                .hpan("hpan")
                .merchantId("0")
                .circuitType("00")
                .mcc("813")
                .idTrxIssuer("0")
                .amountCurrency("833")
                .correlationId("1")
                .acquirerId("0")
                .terminalId("0")
                .bin("0000")
                .par("par")
                .build();
    }

}