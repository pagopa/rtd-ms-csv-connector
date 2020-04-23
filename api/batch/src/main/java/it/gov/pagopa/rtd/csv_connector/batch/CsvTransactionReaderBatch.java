package it.gov.pagopa.rtd.csv_connector.batch;

import com.zaxxer.hikari.HikariDataSource;
import it.gov.pagopa.rtd.csv_connector.batch.encryption.exception.PGPDecryptException;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundTransactionFieldSetMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.batch.scheduler.CsvTransactionReaderTaskScheduler;
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
import org.springframework.transaction.PlatformTransactionManager;

import java.io.FileNotFoundException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Autowired
    private HikariDataSource dataSource;

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

        if (log.isDebugEnabled()) {
            log.debug("Scheduled job ended with status: " + jobExecution.getStatus());
        }

    }

    @Bean
    public PlatformTransactionManager getTransactionManager() {
        DataSourceTransactionManager dataSourceTransactionManager = new DataSourceTransactionManager();
        dataSourceTransactionManager.setDataSource(dataSource);
        return dataSourceTransactionManager;
    }

    @Bean
    public JobRepository getJobRepository() throws Exception {
        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setTransactionManager(getTransactionManager());
        jobRepositoryFactoryBean.setDataSource(dataSource);
        return jobRepositoryFactoryBean.getObject();
    }

    @Bean
    public JobLauncher transactionJobLauncher() throws Exception {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(getJobRepository());
        return simpleJobLauncher;
    }

    @Bean
    public LineTokenizer transactionLineTokenizer() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(";");
        delimitedLineTokenizer.setNames(
                "codice_acquirer", "tipo_operazione", "tipo_circuito", "PAN", "timestamp", "id_trx_acquirer",
                "id_trx_issuer", "correlation_id", "importo", "currency", "acquirerID", "merchantID", "MCC");
        return delimitedLineTokenizer;
    }

    @Bean
    public FieldSetMapper<InboundTransaction> transactionFieldSetMapper() {
        return new InboundTransactionFieldSetMapper(timestampPattern);
    }


    @Bean
    public LineMapper<InboundTransaction> transactionLineMapper() {
        DefaultLineMapper<InboundTransaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(transactionLineTokenizer());
        lineMapper.setFieldSetMapper(transactionFieldSetMapper());
        return lineMapper;
    }


    @SneakyThrows
    @Bean
    @StepScope
    public FlatFileItemReader<InboundTransaction> transactionItemReader(
            @Value("#{stepExecutionContext['fileName']}") String file) {
        PGPFlatFileItemReader flatFileItemReader = new PGPFlatFileItemReader(secretKeyPath, passphrase);
        flatFileItemReader.setResource(new UrlResource(file));
        flatFileItemReader.setLineMapper(transactionLineMapper());
        flatFileItemReader.setLinesToSkip(linesToSkip);
        return flatFileItemReader;
    }


    @Bean
    @StepScope
    public ItemProcessor<InboundTransaction, Transaction> transactionItemProcessor() {
        return beanFactory.getBean(InboundTransactionItemProcessor.class);
    }

    @Bean
    @StepScope
    public ItemWriter<Transaction> getItemWriter() {
        return beanFactory.getBean(TransactionWriter.class);
    }

    @SneakyThrows
    @Bean
    public Step archivalTask() {
        ArchivalTasklet archivalTasklet = new ArchivalTasklet();
        archivalTasklet.setSuccessPath(successArchivePath);
        archivalTasklet.setErrorPath(errorArchivePath);
        return stepBuilderFactory.get("csv-success-archive-step").tasklet(archivalTasklet).build();
    }

    @SneakyThrows
    public FlowJobBuilder transactionJobBuilder() {
        return jobBuilderFactory.get("csv-transaction-job")
                .repository(getJobRepository())
                .start(masterStep()).on("*").to(archivalTask())
                .build();
    }

    @Bean
    @JobScope
    public Partitioner partitioner() throws Exception {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        partitioner.setResources(resolver.getResources(directoryPath));
        partitioner.partition(partitionerSize);
        return partitioner;
    }

    @Bean
    public Step masterStep() throws Exception {
        return stepBuilderFactory.get("csv-transaction-connector-master-step").partitioner(workerStep())
                .partitioner("partition", partitioner())
                .taskExecutor(partitionerTaskExecutor()).build();
    }

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

    @Bean
    public TaskExecutor partitionerTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(partitionerMaxPoolSize);
        taskExecutor.setCorePoolSize(partitionerCorePoolSize);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @Bean
    public TaskExecutor readerTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(readerMaxPoolSize);
        taskExecutor.setCorePoolSize(readerCorePoolSize);
        taskExecutor.afterPropertiesSet();
        return taskExecutor;
    }

    @SneakyThrows
    @Bean
    public Job job() {
       return transactionJobBuilder().build();
    }

    @Bean
    public TaskScheduler poolScheduler() {
        return beanFactory.getBean(CsvTransactionReaderTaskScheduler.class);
    }

    public AtomicInteger getBatchRunCounter() {
        return batchRunCounter;
    }

}
