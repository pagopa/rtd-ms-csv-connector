package it.gov.pagopa.rtd.csv_connector.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * @author ALessio Cialini
 * Configuration class for the CsvTransactionPublisherConnector
 */
@Configuration
@PropertySource("classpath:config/csvTransactionPublisher.properties")
public class CsvTransactionPublisherConfig { }
