package it.gov.pagopa.rtd.csv_connector.batch.config;

import it.gov.pagopa.rtd.csv_connector.batch.PaymentInstrumentRemovalBatch;
import it.gov.pagopa.rtd.csv_connector.batch.step.PaymentInstrumentWriter;
import it.gov.pagopa.rtd.csv_connector.connector.config.CsvConnectorBatchJpaConfig;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentConnector;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentRestClient;
import it.gov.pagopa.rtd.csv_connector.service.PaymentInstrumentConnectorService;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

/**
 * Test configuration class for api/event
 */

@ComponentScan(basePackages = {"it.gov.pagopa"}, excludeFilters = {
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= PaymentInstrumentRemovalBatch.class),
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= PaymentInstrumentConnector.class),
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= PaymentInstrumentRestClient.class),
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= PaymentInstrumentWriter.class),
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= PaymentInstrumentConnectorService.class),
        @ComponentScan.Filter(type= FilterType.ASSIGNABLE_TYPE, value= CsvConnectorBatchJpaConfig.class)
})
public class CsvTransactionReaderTestConfig {}
