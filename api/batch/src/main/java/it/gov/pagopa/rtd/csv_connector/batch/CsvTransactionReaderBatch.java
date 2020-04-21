package it.gov.pagopa.rtd.csv_connector.batch;

import com.zaxxer.hikari.HikariDataSource;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.InboundTransactionFieldSetMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import it.gov.pagopa.rtd.csv_connector.batch.scheduler.CsvTransactionReaderTaskScheduler;
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
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
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
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
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
    @Value("${batchConfiguration.CsvTransactionReaderBatch.secretKeyPath}")
    private String secretKeyPath;
    @Value("${batchConfiguration.CsvTransactionReaderBatch.passphrase}")
    private String passphrase;

    @Autowired
    private HikariDataSource dataSource;

    @Scheduled(cron = "${batchConfiguration.CsvTransactionReaderBatch.cron}")
    public void launchJob() throws Exception {

        Date startDate = new Date();
        if (log.isInfoEnabled()) {
            log.info("CsvTransactionReader scheduled job started at " + startDate);
        }

        JobExecution jobExecution = getJobLauncher().run(
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
    public JobLauncher getJobLauncher() throws Exception {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(getJobRepository());
        return simpleJobLauncher;
    }

    @Bean
    public LineTokenizer getLineTokenizer() {
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(";");
        delimitedLineTokenizer.setNames(
                "codice_acquirer", "tipo_operazione", "tipo_circuito", "PAN", "timestamp", "id_trx_acquirer",
                "id_trx_issuer", "correlation_id", "importo", "currency", "acquirerID", "merchantID", "MCC");
        return delimitedLineTokenizer;
    }

    @Bean
    public FieldSetMapper<InboundTransaction> getFieldSetMapper() {
        return new InboundTransactionFieldSetMapper();
    }


    @Bean
    public LineMapper<InboundTransaction> getLineMapper() {
        DefaultLineMapper<InboundTransaction> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(getLineTokenizer());
        lineMapper.setFieldSetMapper(getFieldSetMapper());
        return lineMapper;
    }


    @Bean
    public ItemReader<InboundTransaction> getItemReader() throws IOException {
        MultiResourceItemReader<InboundTransaction> multiResourceItemReader =
                new MultiResourceItemReader<>();
        ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        multiResourceItemReader.setResources(patternResolver.getResources(directoryPath));

        PGPFlatFileItemReader delegateReader = new PGPFlatFileItemReader(secretKeyPath, passphrase);
        delegateReader.setLineMapper(getLineMapper());

        multiResourceItemReader.setDelegate(delegateReader);
        return multiResourceItemReader;
    }

    @Bean
    public ItemWriter<Transaction> getItemWriter() {
        return beanFactory.getBean(TransactionWriter.class);
    }


    @Bean
    public ItemProcessor<InboundTransaction, Transaction> getItemProcessor() {
        return beanFactory.getBean(InboundTransactionItemProcessor.class);
    }

    @Bean
    public Step pgpReaderStep() {
        try {
            return stepBuilderFactory.get("csv-transaction-reader-step")
                    .<InboundTransaction, Transaction>chunk(10)
                    .reader(getItemReader())
                    .processor(getItemProcessor())
                    .writer(getItemWriter())
                    .build();
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error(e.getMessage(),e);
            }
        }

        return null;
    }

    @SneakyThrows
    @Bean
    public Job job() {
        return jobBuilderFactory.get("csv-transaction-job")
                .repository(getJobRepository())
                .start(pgpReaderStep())
                .build();
    }


    @Bean
    public TaskScheduler poolScheduler() {
        return beanFactory.getBean(CsvTransactionReaderTaskScheduler.class);
    }

    public AtomicInteger getBatchRunCounter() {
        return batchRunCounter;
    }

}
