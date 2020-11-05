package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.service.WriterTrackerService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Data
@Slf4j
public class TerminationTasklet implements Tasklet, InitializingBean {

    private final WriterTrackerService writerTrackerService;
    private String errorDir;
    private String publicKeyDir;
    private Boolean applyEncrypt;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        List<CountDownLatch> countDownLatchList = writerTrackerService.getCountDownLatches();

        if (!countDownLatchList.isEmpty()) {

            countDownLatchList.get(countDownLatchList.size()-1).await();

            for (CountDownLatch countDownLatch : countDownLatchList) {
                countDownLatch.await();
            }
        }

        if (applyEncrypt) {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources(errorDir.concat("/*.csv"));
            Resource publicKey = resolver.getResource(publicKeyDir);
            for (Resource resource : resources) {
                FileInputStream publicKeyIS = null;
                FileOutputStream outputFOS = null;
                try {
                    resource.getFilename();
                    publicKeyIS = new FileInputStream(publicKey.getFile());
                    outputFOS = new FileOutputStream(resource.getFile().getAbsolutePath().concat(".pgp"));
                    PGPDecryptUtil.encryptFile(outputFOS,
                            resource.getFile().getAbsolutePath(),
                            PGPDecryptUtil.readPublicKey(publicKeyIS),
                            false, true);
                } finally {
                    if (publicKeyIS != null) {
                        publicKeyIS.close();
                    }
                    if (outputFOS != null) {
                        outputFOS.close();
                    }
                    FileUtils.forceDelete(resource.getFile());
                }
            }
        }


        return RepeatStatus.FINISHED;
    }

    @Override
    public void afterPropertiesSet() throws Exception {}
}
