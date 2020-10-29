package it.gov.pagopa.rtd.csv_connector.batch.config;

import it.gov.pagopa.rtd.transaction_manager.connector.config.CsvConnectorBatchJpaConfig;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Test configuration class for api/event
 */

@ComponentScan(basePackages = {"it.gov.pagopa"}, excludeFilters = {
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= CsvConnectorBatchJpaConfig.class)
})
public class TestConfig {
}
