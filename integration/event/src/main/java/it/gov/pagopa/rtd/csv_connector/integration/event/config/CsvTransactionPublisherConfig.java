package it.gov.pagopa.rtd.csv_connector.integration.event.config;

import it.gov.pagopa.rtd.csv_connector.integration.event.CsvTransactionPublisherConnector;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Configuration class for {@link CsvTransactionPublisherConnector}
 */
@Configuration
@PropertySource("classpath:config/csvTransactionPublisher.properties")
public class CsvTransactionPublisherConfig { }
