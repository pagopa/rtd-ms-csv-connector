package it.gov.pagopa.rtd.csv_connector.batch.step;

import eu.sia.meda.BaseTest;
import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemWriterListener;
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

import java.util.Collections;

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

    @Before
    public void initTest() {
        Mockito.reset(csvTransactionPublisherServiceMock);
        transactionWriter = new TransactionWriter(
                writerTrackerServiceMock, csvTransactionPublisherServiceMock);
        transactionWriter.setTransactionItemWriterListener(transactionItemWriterListenerMock);
        transactionWriter.setExecutorPoolSize(1);
        BDDMockito.doNothing().when(csvTransactionPublisherServiceMock)
                .publishTransactionEvent(Mockito.any(Transaction.class));
        BDDMockito.doNothing().when(writerTrackerServiceMock)
                .addCountDownLatch(Mockito.any());
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

//    @Test
//    public void testWriterEmptyList() {
//        try {
//            transactionWriter.write(Collections.emptyList());
//            BDDMockito.verifyZeroInteractions(csvTransactionPublisherServiceMock);
//        } catch (Exception e) {
//            e.printStackTrace();
//            Assert.fail();
//        }
//    }
//
//    @Test
//    public void testWriterMonoList() {
//        try {
//            transactionWriter.write(Collections.singletonList(Transaction.builder().build()));
//            BDDMockito.verify(csvTransactionPublisherServiceMock, Mockito.times(1))
//                    .publishTransactionEvent(Mockito.any(Transaction.class));
//        } catch (Exception e) {
//            e.printStackTrace();
//            Assert.fail();
//        }
//    }

//    @Test
//    public void testWriterMultiList() {
//        try {
//            transactionWriter.write(Collections.nCopies(5,Transaction.builder().build()));
//            BDDMockito.verify(csvTransactionPublisherServiceMock, Mockito.times(5))
//                    .publishTransactionEvent(Mockito.any(Transaction.class));
//        } catch (Exception e) {
//            e.printStackTrace();
//            Assert.fail();
//        }
//    }

}