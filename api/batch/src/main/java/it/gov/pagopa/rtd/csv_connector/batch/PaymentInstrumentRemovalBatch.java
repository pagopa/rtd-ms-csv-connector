package it.gov.pagopa.rtd.csv_connector.batch;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.exception.PGPDecryptException;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundPaymentInstrumentFieldSetMapper;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundPaymentInstrumentLineMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.step.ArchivalTasklet;
import it.gov.pagopa.rtd.csv_connector.batch.step.InboundPaymentInstrumentItemProcessor;
import it.gov.pagopa.rtd.csv_connector.batch.step.PGPFlatFileItemReader;
import it.gov.pagopa.rtd.csv_connector.batch.step.PaymentInstrumentWriter;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

/**
 * Configuration of a scheduled batch job to read and decrypt .pgp files with csv content,
 * to be processed in instances of Transaction class, to be sent in an outbound Kafka channel
 */

@Data
@Configuration
@PropertySource("classpath:config/paymentInstrumentRemovalBatch.properties")
@EnableBatchProcessing
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class PaymentInstrumentRemovalBatch {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BeanFactory beanFactory;

    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.classpath}")
    private String directoryPath;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.successArchivePath}")
    private String successArchivePath;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.errorArchivePath}")
    private String errorArchivePath;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.applyDecrypt}")
    private Boolean applyDecrypt;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.secretKeyPath}")
    private String secretKeyPath;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.passphrase}")
    private String passphrase;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.partitionerSize}")
    private Integer partitionerSize;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.chunkSize}")
    private Integer chunkSize;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.partitionerMaxPoolSize}")
    private Integer partitionerMaxPoolSize;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.partitionerCorePoolSize}")
    private Integer partitionerCorePoolSize;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.readerMaxPoolSize}")
    private Integer readerMaxPoolSize;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.readerCorePoolSize}")
    private Integer readerCorePoolSize;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.skipLimit}")
    private Integer skipLimit;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.linesToSkip}")
    private Integer linesToSkip;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.timestampPattern}")
    private String timestampPattern;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.tablePrefix}")
    private String tablePrefix;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.errorLogsPath}")
    private String errorLogsPath;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.enableOnReadErrorFileLogging}")
    private Boolean enableOnReadErrorFileLogging;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.enableOnReadErrorLogging}")
    private Boolean enableOnReadErrorLogging;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.enableOnProcessErrorFileLogging}")
    private Boolean enableOnProcessErrorFileLogging;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.enableOnProcessErrorLogging}")
    private Boolean enableOnProcessErrorLogging;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.enableOnWriteErrorFileLogging}")
    private Boolean enableOnWriteErrorFileLogging;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.enableOnWriteErrorLogging}")
    private Boolean enableOnWriteErrorLogging;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.executorPoolSize}")
    private Integer executorPoolSize;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.applyEncrypt}")
    private Boolean applyEncrypt;
    @Value("${batchConfiguration.PaymentInstrumentRemovalBatch.publicKeyPath}")
    private String publicKey;

    private DataSource dataSource;

    /**
     * ScheduTransactionItemProcessListener
     * TransactionItemReaderListener
     * TransactionItemWriterListener
     * TransactionReaderStepListenered method used to launch the configured batch job for processing transaction from a defined directory.
     * The scheduler is based on a cron execution, based on the provided configuration
     * @throws Exception
     */
    @Scheduled(cron = "${batchConfiguration.PaymentInstrumentRemovalBatch.cron}")
    public void launchJob() throws Exception {

        Date startDate = new Date();
        log.info("PaymentInstrumentRemoval scheduled job started at {}", startDate);

        paymentInstrumentJobLauncher().run(
                paymentInstrumentJob(), new JobParametersBuilder()
                        .addDate("startDateTime", startDate)
                        .toJobParameters());

        Date endDate = new Date();

        log.info("PaymentInstrumentRemoval scheduled job ended at {}" , endDate);
        log.info("Completed in: {} (ms)", + (endDate.getTime() - startDate.getTime()));

    }

    /**
     *
     * @return configured instance of TransactionManager
     */
    @Bean
    public PlatformTransactionManager getTransactionManager() {
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager();
        dataSourceTransactionManager.setDataSource(dataSource);
        return dataSourceTransactionManager;
    }

    /**
     *
     * @return configured instance of JobRepository
     * @throws Exception
     */
    @Bean
    public JobRepository getJobRepository() throws Exception {
        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setTransactionManager( getTransactionManager());
        jobRepositoryFactoryBean.setTablePrefix(tablePrefix);
        jobRepositoryFactoryBean.setDataSource(dataSource);
        jobRepositoryFactoryBean.afterPropertiesSet();
        return jobRepositoryFactoryBean.getObject();
    }

    /**
     *
     * @return configured instance of JobLauncher
     * @throws Exception
     */
    @Bean
    public JobLauncher paymentInstrumentJobLauncher() throws Exception {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(getJobRepository());
        return simpleJobLauncher;
    }

    /**
     *
     * @return instance of the LineTokenizer to be used in the itemReader configured for the job
     */
    @Bean
    public LineTokenizer paymentInstrumentLineTokenizer() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(";");
        delimitedLineTokenizer.setNames(
                "fiscal_code", "hpan", "timestamp");
        return delimitedLineTokenizer;
    }

    /**
     *
     * @return instance of the FieldSetMapper to be used in the itemReader configured for the job
     */
    @Bean
    public FieldSetMapper<InboundPaymentInstrument> paymentInstrumentFieldSetMapper() {
        return new InboundPaymentInstrumentFieldSetMapper(timestampPattern);
    }

    /**
     *
     * @return instance of the LineMapper to be used in the itemReader configured for the job
     */
    public LineMapper<InboundPaymentInstrument> paymentInstrumentLineMapper(String file) {
        InboundPaymentInstrumentLineMapper lineMapper = new InboundPaymentInstrumentLineMapper();
        lineMapper.setTokenizer(paymentInstrumentLineTokenizer());
        lineMapper.setFilename(file);
        lineMapper.setFieldSetMapper(paymentInstrumentFieldSetMapper());
        return lineMapper;
    }

    /**
     *
     * @param file
     *          Late-Binding parameter to be used as the resource for the reader instance
     * @return instance of the itemReader to be used in the first step of the configured job
     */
    @SneakyThrows
    @Bean
    @StepScope
    public FlatFileItemReader<InboundPaymentInstrument> paymentInstrumentItemReader(
            @Value("#{stepExecutionContext['fileName']}") String file) {
        PGPFlatFileItemReader<InboundPaymentInstrument> flatFileItemReader =
                new PGPFlatFileItemReader<>(secretKeyPath, passphrase, applyDecrypt);
        flatFileItemReader.setResource(new UrlResource(file));
        flatFileItemReader.setLineMapper(paymentInstrumentLineMapper(file));
        flatFileItemReader.setLinesToSkip(linesToSkip);
        return flatFileItemReader;
    }

    /**
     *
     * @return instance of the itemProcessor to be used in the first step of the configured job
     */
    @Bean
    @StepScope
    public ItemProcessor<InboundPaymentInstrument, InboundPaymentInstrument> paymentInstrumentItemProcessor() {
        return beanFactory.getBean(InboundPaymentInstrumentItemProcessor.class);
    }

    /**
     *
     * @return instance of the itemWriter to be used in the first step of the configured job
     */
    @Bean
    @StepScope
    public ItemWriter<InboundPaymentInstrument> paymentInstrumentItemWriter() {
        return beanFactory.getBean(PaymentInstrumentWriter.class);
    }


    /**
     *
     * @return step instance based on the tasklet to be used for file archival at the end of the reading process
     */
    @Bean
    public Step archivalTask() {
        ArchivalTasklet archivalTasklet = new ArchivalTasklet();
        archivalTasklet.setSuccessPath(successArchivePath);
        archivalTasklet.setErrorPath(errorArchivePath);
        return stepBuilderFactory.get("csv-pm-success-archive-step").tasklet(archivalTasklet).build();
    }

    /**
     *
     * @return instance of the job to process and archive .pgp files containing Transaction data in csv format
     */
    public FlowJobBuilder paymentInstrumentJobBuilder() throws Exception {
        return jobBuilderFactory.get("csv-payment-instrument-job")
                .repository(getJobRepository())
                .start(paymentInstrumentMasterStep()).on("FAILED").to(archivalTask())
                .from(paymentInstrumentMasterStep()).on("*").to(archivalTask())
                .build();
    }

    /**
     *
     * @return instance of a partitioner to be used for processing multiple files from a single directory
     * @throws Exception
     */
    @Bean
    @JobScope
    public Partitioner paymentInstrumentPartitioner() throws IOException {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        partitioner.setResources(resolver.getResources(directoryPath));
        partitioner.partition(partitionerSize);
        return partitioner;
    }

    /**
     *
     * @return master step to be used as the formal main step in the reading phase of the job,
     * partitioned for scalability on multiple file reading
     * @throws Exception
     */
    @Bean
    public Step paymentInstrumentMasterStep() throws IOException {
        return stepBuilderFactory.get("csv-payment-instrument-connector-master-step").partitioner(paymentInstrumentWorkerStep())
                .partitioner("partition", paymentInstrumentPartitioner())
                .taskExecutor(paymentInstrumentPartitionerTaskExecutor()).build();
    }

    /**
     *
     * @return worker step, defined as a standard reader/processor/writer process,
     * using chunk processing for scalability
     * @throws Exception
     */
    @Bean
    public TaskletStep paymentInstrumentWorkerStep() {
        return stepBuilderFactory.get("csv-payment-instrument-connector-master-inner-step")
                .<InboundPaymentInstrument, InboundPaymentInstrument>chunk(chunkSize)
                .reader(paymentInstrumentItemReader(null))
                .processor(paymentInstrumentItemProcessor())
                .writer(paymentInstrumentItemWriter())
                .faultTolerant()
                .skipLimit(skipLimit)
                .noSkip(PGPDecryptException.class)
                .noSkip(FileNotFoundException.class)
                .skip(Exception.class)
                .taskExecutor(aymentInstrumentReaderTaskExecutor())
                .build();
    }

    /**
     *
     * @return bean configured for usage in the partitioner instance of the job
     */
    @Bean
    public TaskExecutor paymentInstrumentPartitionerTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(partitionerMaxPoolSize);
        taskExecutor.setCorePoolSize(partitionerCorePoolSize);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    /**
     *
     * @return bean configured for usage for chunk reading of a single file
     */
    @Bean
    public TaskExecutor aymentInstrumentReaderTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(readerMaxPoolSize);
        taskExecutor.setCorePoolSize(readerCorePoolSize);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    /**
     *
     * @return instance of a job for transaction processing
     */
    @SneakyThrows
    @Bean
    public Job paymentInstrumentJob() {
        return paymentInstrumentJobBuilder().build();
    }

    /**
     *
     * @return bean for a ThreadPoolTaskScheduler
     */
    @Bean
    public TaskScheduler paymentInstrumentPoolScheduler() {
        return new ThreadPoolTaskScheduler();
    }

    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

}
