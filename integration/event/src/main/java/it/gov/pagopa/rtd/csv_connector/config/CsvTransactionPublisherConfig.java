package it.gov.pagopa.rtd.csv_connector.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:config/csvTransactionPublisher.properties")
public class CsvTransactionPublisherConfig { }
