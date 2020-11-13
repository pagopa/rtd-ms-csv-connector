package it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.config;

import it.gov.pagopa.bpd.common.connector.config.RestConnectorConfig;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentConnector;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

@Configuration
@Import(RestConnectorConfig.class)
@EnableFeignClients(clients = PaymentInstrumentConnector.class)
@PropertySource("classpath:config/payment_instrument/rest-client.properties")
public class PaymentInstrumentRestConnectorConfig {

}
