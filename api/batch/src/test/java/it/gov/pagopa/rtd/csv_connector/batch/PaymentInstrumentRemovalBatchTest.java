package it.gov.pagopa.rtd.csv_connector.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.sia.meda.core.properties.PropertiesManager;
import eu.sia.meda.event.configuration.ArchEventConfigurationService;
import eu.sia.meda.event.transformer.SimpleEventRequestTransformer;
import eu.sia.meda.event.transformer.SimpleEventResponseTransformer;
import it.gov.pagopa.rtd.csv_connector.batch.config.CsvPaymentInstrumentRemovalTestConfig;
import it.gov.pagopa.rtd.csv_connector.integration.event.CsvTransactionPublisherConnector;
import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.service.CsvTransactionPublisherService;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.http.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.cloud.openfeign.FeignAutoConfiguration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.management.ManagementFactory;
import java.util.Date;

import static org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED;

/**
 * Class for testing the CsvTransactionReaderBatch class
 */
@RunWith(SpringRunner.class)
@SpringBatchTest
@EmbeddedKafka(
        partitions = 1,
        count = 1,
        controlledShutdown = true
)
@DataJpaTest
@Transactional(propagation = NOT_SUPPORTED)
@Sql({
        "classpath:org/springframework/batch/core/schema-drop-hsqldb.sql",
        "classpath:org/springframework/batch/core/schema-hsqldb.sql"})
@EnableAutoConfiguration
@ContextConfiguration(classes = {
        CsvPaymentInstrumentRemovalTestConfig.class,
        JacksonAutoConfiguration.class,
        AuthenticationConfiguration.class,
        KafkaAutoConfiguration.class,
        ArchEventConfigurationService.class,
        PropertiesManager.class,
        KafkaAutoConfiguration.class,
        SimpleEventRequestTransformer.class,
        SimpleEventResponseTransformer.class,
        FeignAutoConfiguration.class,
        HttpMessageConvertersAutoConfiguration.class,
        PaymentInstrumentRemovalBatch.class
})
@TestPropertySource(
        locations = {
                "classpath:config/testCsvTransactionPublisher.properties",
        },
        properties = {
                "spring.main.allow-bean-definition-overriding=true",
                "batchConfiguration.PaymentInstrumentRemovalBatch.enabled=true",
                "batchConfiguration.PaymentInstrumentRemovalBatch.applyHashing=true",
                "batchConfiguration.PaymentInstrumentRemovalBatch.applyDecrypt=true",
                "batchConfiguration.PaymentInstrumentRemovalBatch.secretKeyPath=classpath:/test-encrypt-pm/secretKey.asc",
                "batchConfiguration.PaymentInstrumentRemovalBatch.passphrase=test",
                "batchConfiguration.PaymentInstrumentRemovalBatch.skipLimit=3",
                "batchConfiguration.PaymentInstrumentRemovalBatch.partitionerMaxPoolSize=1",
                "batchConfiguration.PaymentInstrumentRemovalBatch.partitionerCorePoolSize=1",
                "batchConfiguration.PaymentInstrumentRemovalBatch.readerMaxPoolSize=1",
                "batchConfiguration.PaymentInstrumentRemovalBatch.readerCorePoolSize=1",
                "batchConfiguration.PaymentInstrumentRemovalBatch.classpath=classpath:/test-encrypt-pm/**/*.pgp",
                "batchConfiguration.PaymentInstrumentRemovalBatch.successArchivePath=classpath:/test-encrypt-pm/**/success",
                "batchConfiguration.PaymentInstrumentRemovalBatch.errorArchivePath=classpath:/test-encrypt-pm/**/error",
                "batchConfiguration.PaymentInstrumentRemovalBatch.timestampPattern=MM/dd/yyyy HH:mm:ss",
                "batchConfiguration.PaymentInstrumentRemovalBatch.linesToSkip=0",
        })
public class PaymentInstrumentRemovalBatchTest {

    @Autowired
    ArchEventConfigurationService archEventConfigurationService;
    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaTemplate<String, String> template;
    @Value("${spring.embedded.kafka.brokers}")
    private String bootstrapServers;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @SpyBean
    private CsvTransactionPublisherConnector csvTransactionPublisherConnectorSpy;

    @SpyBean
    private CsvTransactionPublisherService csvTransactionPublisherServiceSpy;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(
            new File(getClass().getResource("/test-encrypt-pm").getFile()));


    @SneakyThrows
    @Before
    public void setUp() {
        Mockito.reset(
                csvTransactionPublisherConnectorSpy,
                csvTransactionPublisherServiceSpy);
        ObjectName kafkaServerMbeanName = new ObjectName("kafka.server:type=app-info,id=0");
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        if (mBeanServer.isRegistered(kafkaServerMbeanName)) {
            mBeanServer.unregisterMBean(kafkaServerMbeanName);
        }
        tempFolder.newFolder("success");
        tempFolder.newFolder("error");
    }

    private JobParameters defaultJobParameters() {
        return new JobParametersBuilder()
                .addDate("startDateTime",  new Date())
                .toJobParameters();
    }

    @Test
    public void testJob_KO_WrongKey() {
        try {

            File testTrxPgp = tempFolder.newFile("wrong-test-trx.pgp");
            FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

            PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                    this.getClass().getResource("/test-encrypt-pm").getFile() + "/test-pm.csv",
                    PGPDecryptUtil.readPublicKey(
                            this.getClass().getResourceAsStream("/test-encrypt-pm/otherPublicKey.asc")),
                    false,false);

            textTrxPgpFOS.close();

            JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());
            Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt-pm/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt-pm/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

            Mockito.verifyZeroInteractions(csvTransactionPublisherServiceSpy);
            Mockito.verifyZeroInteractions(csvTransactionPublisherConnectorSpy);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testJob_Ok_FileWithSkipsOnLimit() {
        try {

            File testTrxPgp = tempFolder.newFile("test-pm.pgp");

            FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

            PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                    this.getClass().getResource("/test-encrypt-pm").getFile() + "/test-pm.csv",
                    PGPDecryptUtil.readPublicKey(
                            this.getClass().getResourceAsStream("/test-encrypt-pm/publicKey.asc")),
                    false,false);

            textTrxPgpFOS.close();

            JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());
            Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt-pm/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt-pm/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testJob_KO_FileOverSkipLimit() {
        try {

            File testTrxPgp = tempFolder.newFile("test-err-pm.pgp");

            FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

            PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                    this.getClass().getResource("/test-encrypt-pm").getFile() + "/test-err-pm.csv",
                    PGPDecryptUtil.readPublicKey(
                            this.getClass().getResourceAsStream("/test-encrypt-pm/publicKey.asc")),
                    false,false);

            textTrxPgpFOS.close();

            JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());
            Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt-pm/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt-pm/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @After
    public void tearDown() {
        tempFolder.delete();
    }

}