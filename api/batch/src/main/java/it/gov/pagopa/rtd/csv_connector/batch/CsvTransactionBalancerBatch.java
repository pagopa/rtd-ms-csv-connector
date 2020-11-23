package it.gov.pagopa.rtd.csv_connector.batch;

import it.gov.pagopa.rtd.csv_connector.batch.step.BalancerTasklet;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Configuration of a scheduled batch job to read and decrypt .pgp files with csv content,
 * to be processed in instances of Transaction class, to be sent in an outbound Kafka channel
 */

@Data
@Configuration
@PropertySource("classpath:config/csvTransactionBalancerBatch.properties")
@EnableBatchProcessing
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class CsvTransactionBalancerBatch {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final BeanFactory beanFactory;
    private AtomicInteger batchRunCounter = new AtomicInteger(0);

    @Value("${batchConfiguration.CsvTransactionBalancerBatch.isolationForCreate}")
    private String isolationForCreate;
    @Value("${batchConfiguration.CsvTransactionBalancerBatch.balancer.enabled}")
    private Boolean enableBalancer;
    @Value("${batchConfiguration.CsvTransactionBalancerBatch.balancer.input}")
    private String balancerInputPath;
    @Value("${batchConfiguration.CsvTransactionBalancerBatch.balancer.output}")
    private List<String> balancerOutputPaths;
    @Value("${batchConfiguration.CsvTransactionBalancerBatch.tablePrefix}")
    private String tablePrefix;

    private DataSource dataSource;

    /**
     * ScheduTransactionItemProcessListener
     * TransactionItemReaderListener
     * TransactionItemWriterListener
     * TransactionReaderStepListenered method used to launch the configured batch job for processing transaction from a defined directory.
     * The scheduler is based on a cron execution, based on the provided configuration
     * @throws  Exception
     */
    @Scheduled(cron = "${batchConfiguration.CsvTransactionBalancerBatch.cron}")
    public void launchJob() throws Exception {

        Date startDate = new Date();
        log.info("CsvTransactionReader scheduled job started at {}", startDate);

        balancerJobLauncher().run(
                balancerJob(), new JobParametersBuilder()
                        .addDate("startDateTime", startDate)
                        .toJobParameters());

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
    public JobLauncher balancerJobLauncher() throws Exception {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(getJobRepository());
        return simpleJobLauncher;
    }

    @Bean
    public Step balancerTask() {
        BalancerTasklet balancerTasklet = new BalancerTasklet();
        balancerTasklet.setIsEnabled(enableBalancer);
        balancerTasklet.setInputPath(balancerInputPath);
        balancerTasklet.setOutputPaths(balancerOutputPaths);
        return stepBuilderFactory.get("csv-balancer-step").tasklet(balancerTasklet).build();
    }

    /**
     *
     * @return instance of the job to process and archive .pgp files containing Transaction data in csv format
     */
    public FlowJobBuilder balancerJobBuilder() throws Exception {
        return jobBuilderFactory.get("csv-transaction-balancer-job")
                .repository(getJobRepository())
                .start(balancerTask()).on("*").end()
                .build();
    }

    /**
     *
     * @return instance of a job for transaction processing
     */
    @SneakyThrows
    @Bean
    public Job balancerJob() {
        return balancerJobBuilder().build();
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
