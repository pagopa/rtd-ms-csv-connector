package it.gov.pagopa.rtd.csv_connector.batch.config;

import it.gov.pagopa.rtd.csv_connector.batch.CsvTransactionBalancerBatch;
import it.gov.pagopa.rtd.csv_connector.batch.CsvTransactionReaderBatch;
import it.gov.pagopa.rtd.csv_connector.batch.PaymentInstrumentRemovalBatch;
import it.gov.pagopa.rtd.csv_connector.connector.config.CsvConnectorBatchJpaConfig;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Test configuration class for api/event
 */

@ComponentScan(basePackages = {"it.gov.pagopa"}, excludeFilters = {
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= CsvTransactionReaderBatch.class),
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= CsvTransactionBalancerBatch.class),
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= CsvConnectorBatchJpaConfig.class)
})
public class CsvPaymentInstrumentRemovalTestConfig {}
