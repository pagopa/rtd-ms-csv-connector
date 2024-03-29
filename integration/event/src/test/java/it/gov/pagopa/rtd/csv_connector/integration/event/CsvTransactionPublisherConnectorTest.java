package it.gov.pagopa.rtd.csv_connector.integration.event;

import eu.sia.meda.event.BaseEventConnectorTest;
import eu.sia.meda.util.TestUtils;
import it.gov.pagopa.rtd.csv_connector.integration.event.model.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

/**
 * Class for unit testing of the CsvTransactionPublisherConnector class
 */
@Import({CsvTransactionPublisherConnector.class})
@TestPropertySource(
        locations = "classpath:config/testCsvTransactionPublisher.properties",
        properties = {

                "connectors.eventConfigurations.items.CsvTransactionPublisherConnector.bootstrapServers=${spring.embedded.kafka.brokers}"
        })
public class CsvTransactionPublisherConnectorTest extends
        BaseEventConnectorTest<Transaction, Boolean, Transaction, Void, CsvTransactionPublisherConnector> {

    @Value("${connectors.eventConfigurations.items.CsvTransactionPublisherConnector.topic}")
    private String topic;

    @Autowired
    private CsvTransactionPublisherConnector csvTransactionPublisherConnector;

    @Override
    protected CsvTransactionPublisherConnector getEventConnector() {
        return csvTransactionPublisherConnector;
    }

    @Override
    protected Transaction getRequestObject() {
        return TestUtils.mockInstance(new Transaction());
    }

    @Override
    protected String getTopic() {
        return topic;
    }

}