package it.gov.pagopa.rtd.csv_connector.batch;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.exception.PGPDecryptException;
import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemProcessListener;
import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemReaderListener;
import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemWriterListener;
import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionReaderStepListener;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundTransactionFieldSetMapper;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundTransactionLineMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.batch.step.*;
import it.gov.pagopa.rtd.csv_connector.service.WriterTrackerService;
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
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Configuration of a scheduled batch job to read and decrypt .pgp files with csv content,
 * to be processed in instances of Transaction class, to be sent in an outbound Kafka channel
 */

@Data
@Configuration
@PropertySource("classpath:config/csvTransactionReaderBatch.properties")
@EnableBatchProcessing
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class CsvTransactionReaderBatch {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BeanFactory beanFactory;
    private AtomicInteger batchRunCounter = new AtomicInteger(0);

    @Value("${batchConfiguration.CsvTransactionReaderBatch.job.name}")
    private String jobName;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.isolationForCreate}")
    private String isolationForCreate;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.classpath}")
    private String directoryPath;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.successArchivePath}")
    private String successArchivePath;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.errorArchivePath}")
    private String errorArchivePath;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.secretKeyPath}")
    private String secretKeyPath;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.passphrase}")
    private String passphrase;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.applyHashing}")
    private Boolean applyHashing;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.applyDecrypt}")
    private Boolean applyDecrypt;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.partitionerSize}")
    private Integer partitionerSize;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.chunkSize}")
    private Integer chunkSize;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.partitionerMaxPoolSize}")
    private Integer partitionerMaxPoolSize;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.partitionerCorePoolSize}")
    private Integer partitionerCorePoolSize;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.readerMaxPoolSize}")
    private Integer readerMaxPoolSize;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.readerCorePoolSize}")
    private Integer readerCorePoolSize;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.skipLimit}")
    private Integer skipLimit;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.linesToSkip}")
    private Integer linesToSkip;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.timestampPattern}")
    private String timestampPattern;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.tablePrefix}")
    private String tablePrefix;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.errorLogsPath}")
    private String errorLogsPath;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.enableOnReadErrorFileLogging}")
    private Boolean enableOnReadErrorFileLogging;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.enableOnReadErrorLogging}")
    private Boolean enableOnReadErrorLogging;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.enableOnProcessErrorFileLogging}")
    private Boolean enableOnProcessErrorFileLogging;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.enableOnProcessErrorLogging}")
    private Boolean enableOnProcessErrorLogging;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.enableOnWriteErrorFileLogging}")
    private Boolean enableOnWriteErrorFileLogging;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.enableOnWriteErrorLogging}")
    private Boolean enableOnWriteErrorLogging;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.executorPoolSize}")
    private Integer executorPoolSize;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.checkpointFrequency}")
    private Integer checkpointFrequency;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.enableCheckpointFrequency}")
    private Boolean enableCheckpointFrequency;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.applyEncrypt}")
    private Boolean applyEncrypt;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.publicKeyPath}")
    private String publicKey;

    private DataSource dataSource;
    private WriterTrackerService writerTrackerService;
    private ExecutorService executorService;

    public void createWriterTrackerService() {
        this.writerTrackerService = writerTrackerService();
    }

    public void clearWriterTrackerService() {
        writerTrackerService.clearAll();
    }

    public WriterTrackerService writerTrackerService() {
        return beanFactory.getBean(WriterTrackerService.class);
    }

    /**
     * ScheduTransactionItemProcessListener
     * TransactionItemReaderListener
     * TransactionItemWriterListener
     * TransactionReaderStepListenered method used to launch the configured batch job for processing transaction from a defined directory.
     * The scheduler is based on a cron execution, based on the provided configuration
     * @throws  Exception
     */
    @Scheduled(cron = "${batchConfiguration.CsvTransactionReaderBatch.cron}")
    public void launchJob() throws Exception {

        Date startDate = new Date();
        log.info("CsvTransactionReader scheduled job started at {}", startDate);

        if (writerTrackerService == null) {
            createWriterTrackerService();
        }

        transactionJobLauncher().run(
                job(), new JobParametersBuilder()
                        .addDate("startDateTime", startDate)
                        .toJobParameters());

        clearWriterTrackerService();

        Date endDate = new Date();

        log.info("CsvTransactionReader scheduled job ended at {}" , endDate);
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
            jobRepositoryFactoryBean.setIsolationLevelForCreate(isolationForCreate);
            jobRepositoryFactoryBean.afterPropertiesSet();
            return jobRepositoryFactoryBean.getObject();
    }

    /**
     *
     * @return configured instance of JobLauncher
     * @throws Exception
     */
    @Bean
    public JobLauncher transactionJobLauncher() throws Exception {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(getJobRepository());
        return simpleJobLauncher;
    }

    /**
     *
     * @return instance of the LineTokenizer to be used in the itemReader configured for the job
     */
    @Bean
    public LineTokenizer transactionLineTokenizer() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(";");
        delimitedLineTokenizer.setNames(
                "codice_acquirer", "tipo_operazione", "tipo_circuito", "PAN", "timestamp", "id_trx_acquirer",
                "id_trx_issuer", "correlation_id", "importo", "currency", "acquirerID", "merchantID", "terminal_id",
                "bank_identification_number", "MCC");
        return delimitedLineTokenizer;
    }

    /**
     *
     * @return instance of the FieldSetMapper to be used in the itemReader configured for the job
     */
    @Bean
    public FieldSetMapper<InboundTransaction> transactionFieldSetMapper() {
        return new InboundTransactionFieldSetMapper(timestampPattern);
    }

    /**
     *
     * @return instance of the LineMapper to be used in the itemReader configured for the job
     */
    public LineMapper<InboundTransaction> transactionLineMapper(String file) {
        InboundTransactionLineMapper lineMapper = new InboundTransactionLineMapper();
        lineMapper.setTokenizer(transactionLineTokenizer());
        lineMapper.setFilename(file);
        lineMapper.setFieldSetMapper(transactionFieldSetMapper());
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
    public FlatFileItemReader<InboundTransaction> transactionItemReader(
            @Value("#{stepExecutionContext['fileName']}") String file) {
        PGPFlatFileItemReader flatFileItemReader = new PGPFlatFileItemReader(secretKeyPath, passphrase, applyDecrypt);
        flatFileItemReader.setResource(new UrlResource(file));
        flatFileItemReader.setLineMapper(transactionLineMapper(file));
        flatFileItemReader.setLinesToSkip(linesToSkip);
        return flatFileItemReader;
    }

    /**
     *
     * @return instance of the itemProcessor to be used in the first step of the configured job
     */
    @Bean
    @StepScope
    public ItemProcessor<InboundTransaction, InboundTransaction> transactionItemProcessor() {
        return beanFactory.getBean(InboundTransactionItemProcessor.class);
    }

    /**
     *
     * @return instance of the itemWriter to be used in the first step of the configured job
     */
    @Bean
    @StepScope
    public ItemWriter<InboundTransaction> getItemWriter(TransactionItemWriterListener writerListener) {
        TransactionWriter transactionWriter = beanFactory.getBean(TransactionWriter.class, writerTrackerService);
        transactionWriter.setTransactionItemWriterListener(writerListener);
        transactionWriter.setApplyHashing(applyHashing);
        transactionWriter.setExecutor(writerExecutor());
        transactionWriter.setCheckpointFrequency(checkpointFrequency);
        transactionWriter.setEnableCheckpointFrequency(enableCheckpointFrequency);
        return transactionWriter;
    }

    /**
     *
     * @return step instance based on the tasklet to be used for file archival at the end of the reading process
     */
    @Bean
    public Step terminationTask() {
        if (writerTrackerService == null) {
            createWriterTrackerService();
        }
        TerminationTasklet terminationTasklet = new TerminationTasklet(writerTrackerService);
        return stepBuilderFactory.get("csv-success-termination-step").tasklet(terminationTasklet).build();
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
        archivalTasklet.setApplyEncrypt(false);
        archivalTasklet.setErrorDir(errorLogsPath);
        archivalTasklet.setPublicKeyDir(publicKey);
        archivalTasklet.setApplyArchive(false);
        return stepBuilderFactory.get("csv-success-archive-step").tasklet(archivalTasklet).build();
    }

    /**
     *
     * @return instance of the job to process and archive .pgp files containing Transaction data in csv format
     */
    public FlowJobBuilder transactionJobBuilder() throws Exception {
        return jobBuilderFactory.get(jobName)
                .repository(getJobRepository())
                .start(masterStep()).on("*").to(terminationTask()).on("*").to(archivalTask())
                .build();
    }

    /**
     *
     * @return instance of a partitioner to be used for processing multiple files from a single directory
     * @throws Exception
     */
    @Bean
    @JobScope
    public Partitioner partitioner() throws IOException {
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
    public Step masterStep() throws IOException {
        return stepBuilderFactory.get("csv-transaction-connector-master-step")
                .partitioner(workerStep(writerTrackerService))
                .partitioner("partition", partitioner())
                .taskExecutor(partitionerTaskExecutor()).build();
    }

    /**
     *
     * @return worker step, defined as a standard reader/processor/writer process,
     * using chunk processing for scalability
     * @throws Exception
     */
    @Bean
    public TaskletStep workerStep(WriterTrackerService writerTrackerService) {
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
        String executionDate = OffsetDateTime.now().format(fmt);

        return stepBuilderFactory.get("csv-transaction-connector-master-inner-step").<InboundTransaction, InboundTransaction>chunk(chunkSize)
                .reader(transactionItemReader(null))
                .processor(transactionItemProcessor())
                .writer(getItemWriter(transactionsItemWriteListener(executionDate)))
                .faultTolerant()
                .skipLimit(skipLimit)
                .noSkip(PGPDecryptException.class)
                .noSkip(FileNotFoundException.class)
                .skip(Exception.class)
                .listener(transactionItemReaderListener(executionDate))
                .listener(transactionsItemWriteListener(executionDate))
                .listener(transactionsItemProcessListener(executionDate))
                .listener(transactionStepListener(writerTrackerService))
                .taskExecutor(readerTaskExecutor())
                .build();
    }

    @Bean
    public TransactionItemReaderListener transactionItemReaderListener(String executionDate) {
        TransactionItemReaderListener transactionItemReaderListener = new TransactionItemReaderListener();
        transactionItemReaderListener.setExecutionDate(executionDate);
        transactionItemReaderListener.setErrorTransactionsLogsPath(errorLogsPath);
        transactionItemReaderListener.setEnableOnErrorFileLogging(enableOnReadErrorFileLogging);
        transactionItemReaderListener.setEnableOnErrorLogging(enableOnReadErrorLogging);
        return transactionItemReaderListener;
    }

    @Bean
    public TransactionItemWriterListener transactionsItemWriteListener(String executionDate) {
        TransactionItemWriterListener transactionsItemWriteListener = new TransactionItemWriterListener();
        transactionsItemWriteListener.setExecutionDate(executionDate);
        transactionsItemWriteListener.setErrorTransactionsLogsPath(errorLogsPath);
        transactionsItemWriteListener.setEnableOnErrorFileLogging(enableOnWriteErrorFileLogging);
        transactionsItemWriteListener.setEnableOnErrorLogging(enableOnWriteErrorLogging);
        return transactionsItemWriteListener;
    }

    @Bean
    public TransactionItemProcessListener transactionsItemProcessListener(String executionDate) {
        TransactionItemProcessListener transactionItemProcessListener = new TransactionItemProcessListener();
        transactionItemProcessListener.setExecutionDate(executionDate);
        transactionItemProcessListener.setErrorTransactionsLogsPath(errorLogsPath);
        transactionItemProcessListener.setEnableOnErrorFileLogging(enableOnProcessErrorFileLogging);
        transactionItemProcessListener.setEnableOnErrorLogging(enableOnProcessErrorLogging);
        return transactionItemProcessListener;
    }

    @Bean
    public TransactionReaderStepListener transactionStepListener(WriterTrackerService writerTrackerService) {
        TransactionReaderStepListener transactionReaderStepListener = new TransactionReaderStepListener();
        transactionReaderStepListener.setErrorPath(errorArchivePath);
        transactionReaderStepListener.setSuccessPath(successArchivePath);
        transactionReaderStepListener.setWriterTrackerService(writerTrackerService);
        transactionReaderStepListener.setApplyEncrypt(applyEncrypt);
        transactionReaderStepListener.setErrorDir(errorLogsPath);
        transactionReaderStepListener.setPublicKeyDir(publicKey);
        return transactionReaderStepListener;
    }

    /**
     *
     * @return bean configured for usage in the partitioner instance of the job
     */
    @Bean
    public TaskExecutor partitionerTaskExecutor() {
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
    public Executor writerExecutor() {
        if (this.executorService == null) {
            executorService =  Executors.newFixedThreadPool(executorPoolSize);
        }
        return executorService;
    }

    /**
     *
     * @return bean configured for usage for chunk reading of a single file
     */
    public TaskExecutor readerTaskExecutor() {
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
    public Job job() {
        return transactionJobBuilder().build();
    }

    /**
     *
     * @return bean for a ThreadPoolTaskScheduler
     */
    @Bean
    public TaskScheduler poolScheduler() {
        return new ThreadPoolTaskScheduler();
    }

    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

}
