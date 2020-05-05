package it.gov.pagopa.rtd.csv_connector.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.sia.meda.core.properties.PropertiesManager;
import eu.sia.meda.event.configuration.ArchEventConfigurationService;
import eu.sia.meda.event.transformer.SimpleEventRequestTransformer;
import eu.sia.meda.event.transformer.SimpleEventResponseTransformer;
import it.gov.pagopa.rtd.csv_connector.CsvTransactionPublisherConnector;
import it.gov.pagopa.rtd.csv_connector.batch.config.TestConfig;
import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.batch.step.InboundTransactionItemProcessor;
import it.gov.pagopa.rtd.csv_connector.batch.step.TransactionWriter;
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
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileOutputStream;
import java.lang.management.ManagementFactory;
import java.util.Date;

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
@EnableAutoConfiguration
@ContextConfiguration(classes = {
        TestConfig.class,
        JacksonAutoConfiguration.class,
        AuthenticationConfiguration.class,
        KafkaAutoConfiguration.class,
        ArchEventConfigurationService.class,
        PropertiesManager.class,
        KafkaAutoConfiguration.class,
        SimpleEventRequestTransformer.class,
        SimpleEventResponseTransformer.class,
        CsvTransactionReaderBatch.class
})
@TestPropertySource(
        locations = "classpath:config/testCsvTransactionPublisher.properties",
        properties = {

                "batchConfiguration.CsvTransactionReaderBatch.secretKeyPath=classpath:/test-encrypt/secretKey.asc",
                "batchConfiguration.CsvTransactionReaderBatch.passphrase=test",
                "batchConfiguration.CsvTransactionReaderBatch.skipLimit=3",
                "batchConfiguration.CsvTransactionReaderBatch.classpath=classpath:/test-encrypt/**/*.pgp",
                "batchConfiguration.CsvTransactionReaderBatch.successArchivePath=classpath:/test-encrypt/**/success",
                "batchConfiguration.CsvTransactionReaderBatch.errorArchivePath=classpath:/test-encrypt/**/error",
                "connectors.eventConfigurations.items.CsvTransactionPublisherConnector.bootstrapServers=${spring.embedded.kafka.brokers}"
        })
public class CsvTransactionReaderBatchTest {

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

    @SpyBean
    private InboundTransactionItemProcessor inboundTransactionItemProcessorSpy;

    @SpyBean
    private TransactionWriter transactionWriterSpy;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(
            new File(getClass().getResource("/test-encrypt").getFile()));


    @SneakyThrows
    @Before
    public void setUp() {
        Mockito.reset(
                csvTransactionPublisherConnectorSpy,
                csvTransactionPublisherServiceSpy,
                inboundTransactionItemProcessorSpy,
                transactionWriterSpy);
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
                    this.getClass().getResource("/test-encrypt").getFile() + "/test-trx.csv",
                    PGPDecryptUtil.readPublicKey(
                            this.getClass().getResourceAsStream("/test-encrypt/otherPublicKey.asc")),
                    false,false);

            textTrxPgpFOS.close();

            JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());
            Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

            Mockito.verifyZeroInteractions(inboundTransactionItemProcessorSpy);
            Mockito.verifyZeroInteractions(csvTransactionPublisherServiceSpy);
            Mockito.verifyZeroInteractions(csvTransactionPublisherConnectorSpy);
            Mockito.verifyZeroInteractions(transactionWriterSpy);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testJob_Ok_NoSkips() {
        try {

            File testTrxPgp = tempFolder.newFile("test-trx.pgp");

            FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

            PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                    this.getClass().getResource("/test-encrypt").getFile() + "/test-trx-ns.csv",
                    PGPDecryptUtil.readPublicKey(
                            this.getClass().getResourceAsStream("/test-encrypt/publicKey.asc")),
                    false,false);

            textTrxPgpFOS.close();

            JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());
            Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

            Mockito.verify(inboundTransactionItemProcessorSpy, Mockito.times(4))
                    .process(Mockito.any());
            Mockito.verify(transactionWriterSpy, Mockito.times(4))
                    .write(Mockito.any());
            Mockito.verify(csvTransactionPublisherServiceSpy, Mockito.times(4))
                    .publishTransactionEvent(Mockito.any());
            Mockito.verify(csvTransactionPublisherConnectorSpy, Mockito.times(4))
                    .doCall(Mockito.any(), Mockito.any(), Mockito.any());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testJob_Ok_FileWithSkipsOnLimit() {
        try {

            File testTrxPgp = tempFolder.newFile("test-trx.pgp");

            FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

            PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                    this.getClass().getResource("/test-encrypt").getFile() + "/test-trx.csv",
                    PGPDecryptUtil.readPublicKey(
                            this.getClass().getResourceAsStream("/test-encrypt/publicKey.asc")),
                    false,false);

            textTrxPgpFOS.close();

            JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());
            Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

            Mockito.verify(inboundTransactionItemProcessorSpy, Mockito.atLeast(2))
                    .process(Mockito.any());
            Mockito.verify(transactionWriterSpy, Mockito.atMost(2))
                    .write(Mockito.any());
            Mockito.verify(csvTransactionPublisherServiceSpy, Mockito.atMost(1))
                    .publishTransactionEvent(Mockito.any());
            Mockito.verify(csvTransactionPublisherConnectorSpy, Mockito.atMost(1))
                    .doCall(Mockito.any(), Mockito.any(), Mockito.any());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testJob_KO_FileOverSkipLimit() {
        try {

            File testTrxPgp = tempFolder.newFile("test-err-trx.pgp");

            FileOutputStream textTrxPgpFOS = new FileOutputStream(testTrxPgp);

            PGPDecryptUtil.encryptFile(textTrxPgpFOS,
                    this.getClass().getResource("/test-encrypt").getFile() + "/test-err-trx.csv",
                    PGPDecryptUtil.readPublicKey(
                            this.getClass().getResourceAsStream("/test-encrypt/publicKey.asc")),
                    false,false);

            textTrxPgpFOS.close();

            JobExecution jobExecution = jobLauncherTestUtils.launchJob(defaultJobParameters());
            Assert.assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Assert.assertEquals(0,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/success")[0].getFile(),
                            new String[]{"pgp"},false).size());
            Assert.assertEquals(1,
                    FileUtils.listFiles(
                            resolver.getResources("classpath:/test-encrypt/**/error")[0].getFile(),
                            new String[]{"pgp"},false).size());

            Mockito.verify(inboundTransactionItemProcessorSpy, Mockito.atMost(5))
                    .process(Mockito.any());
            Mockito.verify(transactionWriterSpy, Mockito.atMost(2))
                    .write(Mockito.any());
            Mockito.verify(csvTransactionPublisherServiceSpy, Mockito.atMost(1))
                    .publishTransactionEvent(Mockito.any());
            Mockito.verify(csvTransactionPublisherConnectorSpy, Mockito.atMost(1))
                    .doCall(Mockito.any(), Mockito.any(), Mockito.any());

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