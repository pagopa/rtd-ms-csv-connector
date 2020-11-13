package it.gov.pagopa.rtd.csv_connector.connector.payment_instrument;

import com.github.tomakehurst.wiremock.extension.responsetemplating.ResponseTemplateTransformer;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import it.gov.pagopa.bpd.common.connector.BaseFeignRestClientTest;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.config.PaymentInstrumentRestConnectorConfig;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
import lombok.SneakyThrows;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.http.HttpMessageConvertersAutoConfiguration;
import org.springframework.cloud.openfeign.FeignAutoConfiguration;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.support.TestPropertySourceUtils;


import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

@TestPropertySource(
        locations = "classpath:config/payment_instrument/rest-client.properties",
        properties = {
                "spring.application.name=bpd-ms-enrollment-integration-rest",
                "feign.client.config.bpd-ms-payment-instrument.connectTimeout=10000",
                "feign.client.config.bpd-ms-payment-instrument.readTimeout=10000",
                "logging.level.it.gov.pagopa.bpd.enrollment=DEBUG"
        })
@ContextConfiguration(initializers = it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.PaymentInstrumentRestClientImplTest.RandomPortInitializer.class,
        classes = {
            PaymentInstrumentRestConnectorConfig.class,
            PaymentInstrumentConnector.class,
            PaymentInstrumentRestClientImpl.class,
            PaymentInstrumentRestClient.class,
            FeignAutoConfiguration.class,
            HttpMessageConvertersAutoConfiguration.class
        }
)
public class PaymentInstrumentRestClientImplTest extends BaseFeignRestClientTest {

    @ClassRule
    public static WireMockClassRule wireMockRule;

    static {
        String port = System.getenv("WIREMOCKPORT");
        wireMockRule = new WireMockClassRule(wireMockConfig()
                .port(port != null ? Integer.parseInt(port) : 0)
                .bindAddress("localhost")
                .usingFilesUnderClasspath("stubs/payment-instrument")
                .extensions(new ResponseTemplateTransformer(false))
        );
    }

    @Test
    public void delete() {
        restClient.delete(PaymentInstrumentData.builder().hpan("test").fiscalCode("test").cancellationDate("2020-04-09T16:22:45.304Z").build());
    }


    @Autowired
    private PaymentInstrumentRestClient restClient;

    public static class RandomPortInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @SneakyThrows
        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            TestPropertySourceUtils
                    .addInlinedPropertiesToEnvironment(applicationContext,
                            String.format("rest-client.payment-instrument.base-url=http://%s:%d/bpd/payment-instruments",
                                    wireMockRule.getOptions().bindAddress(),
                                    wireMockRule.port())
                    );
        }
    }
}