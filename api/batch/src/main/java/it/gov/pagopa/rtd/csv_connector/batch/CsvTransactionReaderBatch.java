package it.gov.pagopa.rtd.csv_connector.batch;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.exception.PGPDecryptException;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundTransactionFieldSetMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.batch.step.ArchivalTasklet;
import it.gov.pagopa.rtd.csv_connector.batch.step.InboundTransactionItemProcessor;
import it.gov.pagopa.rtd.csv_connector.batch.step.PGPFlatFileItemReader;
import it.gov.pagopa.rtd.csv_connector.batch.step.TransactionWriter;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
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
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
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
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Configuration of a scheduled batch job to read and decrypt .pgp files with csv content,
 * to be processed in instances of Transaction class, to be sent in an outbound Kafka channel
 */

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

    private DataSource dataSource;

    /**
     * Scheduled method used to launch the configured batch job for processing transaction from a defined directory.
     * The scheduler is based on a cron execution, based on the provided configuration
     * @throws Exception
     */
    @Scheduled(cron = "${batchConfiguration.CsvTransactionReaderBatch.cron}")
    public void launchJob() throws Exception {

        Date startDate = new Date();
        if (log.isInfoEnabled()) {
            log.info("CsvTransactionReader scheduled job started at " + startDate);
        }

        JobExecution jobExecution = transactionJobLauncher().run(
                job(), new JobParametersBuilder()
                        .addDate("startDateTime", startDate)
                        .toJobParameters());
        batchRunCounter.incrementAndGet();

        Date endDate = new Date();
        if (log.isInfoEnabled()) {
            log.info("CsvTransactionReader scheduled job ended at " + endDate);
            log.info("Completed in: " + (endDate.getTime() - startDate.getTime()) + " (ms)");
        }

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
    @Bean
    public LineMapper<InboundTransaction> transactionLineMapper() {
        DefaultLineMapper<InboundTransaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(transactionLineTokenizer());
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
        flatFileItemReader.setLineMapper(transactionLineMapper());
        flatFileItemReader.setLinesToSkip(linesToSkip);
        return flatFileItemReader;
    }

    /**
     *
     * @return instance of the itemProcessor to be used in the first step of the configured job
     */
    @Bean
    @StepScope
    public ItemProcessor<InboundTransaction, Transaction> transactionItemProcessor() {
        InboundTransactionItemProcessor inboundTransactionItemProcessor =
                beanFactory.getBean(InboundTransactionItemProcessor.class);
        inboundTransactionItemProcessor.setApplyHashing(applyHashing);
        return inboundTransactionItemProcessor;
    }

    /**
     *
     * @return instance of the itemWriter to be used in the first step of the configured job
     */
    @Bean
    @StepScope
    public ItemWriter<Transaction> getItemWriter() {
        return beanFactory.getBean(TransactionWriter.class);
    }

    /**
     *
     * @return step instance based on the tasklet to be used for file archival at the end of the reading process
     */
    @SneakyThrows
    @Bean
    public Step archivalTask() {
        ArchivalTasklet archivalTasklet = new ArchivalTasklet();
        archivalTasklet.setSuccessPath(successArchivePath);
        archivalTasklet.setErrorPath(errorArchivePath);
        return stepBuilderFactory.get("csv-success-archive-step").tasklet(archivalTasklet).build();
    }

    /**
     *
     * @return instance of the job to process and archive .pgp files containing Transaction data in csv format
     */
    @SneakyThrows
    public FlowJobBuilder transactionJobBuilder() {
        return jobBuilderFactory.get("csv-transaction-job")
                .repository(getJobRepository())
                .start(masterStep()).on("*").to(archivalTask())
                .build();
    }

    /**
     *
     * @return instance of a partitioner to be used for processing multiple files from a single directory
     * @throws Exception
     */
    @Bean
    @JobScope
    public Partitioner partitioner() throws Exception {
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
    public Step masterStep() throws Exception {
        return stepBuilderFactory.get("csv-transaction-connector-master-step").partitioner(workerStep())
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
    public Step workerStep() throws Exception {
        return stepBuilderFactory.get("csv-transaction-connector-master-inner-step").<InboundTransaction, Transaction>chunk(chunkSize)
                .reader(transactionItemReader(null))
                .processor(transactionItemProcessor())
                .writer(getItemWriter())
                .faultTolerant()
                .skipLimit(skipLimit)
                .noSkip(PGPDecryptException.class)
                .noSkip(FileNotFoundException.class)
                .skip(Exception.class)
                .taskExecutor(readerTaskExecutor())
                .build();
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
