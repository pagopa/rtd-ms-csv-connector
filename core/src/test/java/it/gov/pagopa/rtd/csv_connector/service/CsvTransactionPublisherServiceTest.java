package it.gov.pagopa.rtd.csv_connector.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.sia.meda.BaseSpringTest;
import eu.sia.meda.event.transformer.SimpleEventRequestTransformer;
import eu.sia.meda.event.transformer.SimpleEventResponseTransformer;
import it.gov.pagopa.rtd.csv_connector.CsvTransactionPublisherConnector;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * Class for unit testing of {@link CsvTransactionPublisherService}
 */
@ContextConfiguration(classes = CsvTransactionPublisherServiceImpl.class)
public class CsvTransactionPublisherServiceTest extends BaseSpringTest {

    @SpyBean
    ObjectMapper objectMapper;

    @MockBean
    private CsvTransactionPublisherConnector csvTransactionPublisherConnectorMock;

    @SpyBean
    private SimpleEventRequestTransformer<Transaction> simpleEventRequestTransformerSpy;

    @SpyBean
    private SimpleEventResponseTransformer simpleEventResponseTransformerSpy;

    @Autowired
    CsvTransactionPublisherService csvTransactionPublisherService;

    private Transaction transaction;

    @Before
    public void setUp() throws Exception {
        Mockito.reset(
                csvTransactionPublisherConnectorMock,
                simpleEventRequestTransformerSpy,
                simpleEventResponseTransformerSpy);
        transaction = getRequestObject();
    }

    @Test
    public void publishTransactionEvent() {

        BDDMockito.doReturn(true)
                .when(csvTransactionPublisherConnectorMock)
                .doCall(Mockito.eq(transaction),
                        Mockito.eq(simpleEventRequestTransformerSpy),
                        Mockito.eq(simpleEventResponseTransformerSpy),
                        Mockito.any());

        try {
            csvTransactionPublisherService.publishTransactionEvent(transaction);
            BDDMockito.verify(csvTransactionPublisherConnectorMock,Mockito.atLeastOnce())
                    .doCall(Mockito.eq(transaction),
                            Mockito.eq(simpleEventRequestTransformerSpy),
                            Mockito.eq(simpleEventResponseTransformerSpy));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }

    protected Transaction getRequestObject() {
        return Transaction.builder()
                .idTrxAcquirer("1")
                .acquirerCode("001")
                .trxDate(OffsetDateTime.parse("2020-04-09T16:22:45.304Z"))
                .amount(BigDecimal.valueOf(1313.13))
                .operationType("00")
                .hpan("hpan")
                .merchantId("0")
                .circuitType("00")
                .mcc("813")
                .idTrxIssuer("0")
                .amountCurrency("833")
                .correlationId("1")
                .acquirerId("0")
                .build();
    }

}
