package it.gov.pagopa.rtd.csv_connector.batch.listener;

import it.gov.pagopa.rtd.csv_connector.batch.encryption.PGPDecryptUtil;
import it.gov.pagopa.rtd.csv_connector.service.WriterTrackerService;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Implementation of {@link StepExecutionListener}, to be used to log and define the exit status of a step
 */

@Slf4j
@Data
public class TransactionReaderStepListener implements StepExecutionListener {

    private WriterTrackerService writerTrackerService;
    private String successPath;
    private String errorPath;
    private String errorDir;
    private String publicKeyDir;
    private Boolean applyEncrypt;
    private String executionDate;
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Starting processing for file: {}", stepExecution.getExecutionContext().get("fileName"));
    }

    @SneakyThrows
    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {

        String file = String.valueOf(stepExecution.getExecutionContext().get("fileName"));

        List<CountDownLatch> countDownLatchList = writerTrackerService.getFileCountDownLatches(file);

        if (countDownLatchList != null) {
            for (CountDownLatch countDownLatch : countDownLatchList) {
                countDownLatch.await();
            }
        }

        String path = null;

        try {
            path = resolver.getResource(file).getFile().getAbsolutePath();
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            path = file.replace("file:/", "");
        }

        try {

            String archivalPath =
                    BatchStatus.COMPLETED.equals(stepExecution.getStatus()) &&
                            stepExecution.getFailureExceptions().size() <= 0 ?
                            successPath : errorPath;

            file = file.replaceAll("\\\\", "/");
            String[] filename = file.split("/");

            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

            archivalPath = resolver.getResources(archivalPath)[0].getFile().getAbsolutePath();

            File destFile = FileUtils.getFile(archivalPath + "/" + RandomUtils.nextLong() +
                    "_" + OffsetDateTime.now().format(fmt) + "_" + filename[filename.length - 1]);

            FileUtils.moveFile(FileUtils.getFile(path), destFile);

            if (applyEncrypt) {
                PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
                Resource[] resources = resolver.getResources(errorDir.concat("/*.csv"));
                Resource publicKey = resolver.getResource(publicKeyDir);
                for (Resource resource : resources) {
                    FileInputStream publicKeyIS = null;
                    FileOutputStream outputFOS = null;
                    try {
                        if (resource.getFilename().contains(filename[filename.length - 1]
                                .replaceAll(".csv", "")
                                .replaceAll(".pgp", ""))) {
                            publicKeyIS = new FileInputStream(publicKey.getFile());
                            outputFOS = new FileOutputStream(resource.getFile()
                                    .getAbsolutePath().concat(".pgp"));
                            PGPDecryptUtil.encryptFile(outputFOS,
                                    resource.getFile().getAbsolutePath(),
                                    PGPDecryptUtil.readPublicKey(publicKeyIS),
                                    false, true);
                        }
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

        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        }


        ExitStatus exitStatus = stepExecution.getExitStatus();

        if (!exitStatus.getExitCode().equals(ExitStatus.FAILED.getExitCode()) &&
                stepExecution.getSkipCount() > 0) {
            exitStatus = new ExitStatus("COMPLETED WITH SKIPS");
        }

        log.info("Processing for file: {} ended with status: {}",
                stepExecution.getExecutionContext().get("fileName"), exitStatus.getExitCode());

        return exitStatus;

    }

}
