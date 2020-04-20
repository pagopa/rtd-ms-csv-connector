package it.gov.pagopa.rtd.csv_connector.batch;

import it.gov.pagopa.rtd.csv_connector.batch.scheduler.CsvTransactionReaderTaskScheduler;
import it.gov.pagopa.rtd.csv_connector.batch.step.PGPItemProcessor;
import it.gov.pagopa.rtd.csv_connector.batch.step.PGPItemReader;
import it.gov.pagopa.rtd.csv_connector.model.Transaction;
import lombok.RequiredArgsConstructor;
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
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableBatchProcessing
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class CsvTransactionReaderBatch {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BeanFactory beanFactory;
    private AtomicInteger batchRunCounter = new AtomicInteger(0);

    //TODO: Apply config path
    private String directoryPath;
    private String secretKeyPath;
    private String passphrase;

    @Scheduled(cron = "0 * 9 * * ?")
    public void launchJob() throws Exception {

        Date startDate = new Date();
        if (log.isInfoEnabled()) {
            log.info("CsvTransactionReader scheduled job started at " + startDate);
        }

        JobExecution jobExecution = jobLauncher().run(
                job(), new JobParametersBuilder()
                        .addDate("startDateTime", startDate)
                        .toJobParameters());
        batchRunCounter.incrementAndGet();

        if (log.isDebugEnabled()) {
            log.debug("Scheduled job ended with status: " + jobExecution.getStatus());
        }

    }

    @Bean
    public JobRepository jobRepository() throws Exception {
        MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean();
        factory.setTransactionManager(transactionManager());
        return (JobRepository) factory.getObject();
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobLauncher jobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository());
        return jobLauncher;
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("csv-transaction-job")
                .start(pgpReaderStep())
                .build();
    }

    @Bean
    public Step pgpReaderStep() {
        try {
            return stepBuilderFactory.get("step").<byte[], Transaction>chunk(10)
                    .reader(pgpItemReader())
                    .processor(pgpItemProcessor())
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Bean
    public PGPItemReader pgpItemReader() throws IOException {
        return new PGPItemReader(directoryPath, secretKeyPath, passphrase);
    }

    @Bean
    public PGPItemProcessor pgpItemProcessor() throws IOException {
        return beanFactory.getBean(PGPItemProcessor.class);
    }

    @Bean
    public TaskScheduler poolScheduler() {
        return beanFactory.getBean(CsvTransactionReaderTaskScheduler.class);
    }

    public AtomicInteger getBatchRunCounter() {
        return batchRunCounter;
    }

}
