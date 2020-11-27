package it.gov.pagopa.rtd.csv_connector.batch.listener;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Implementation of {@link ItemWriteListener}, to be used to log and/or store records
 * that have produced an error while reading a record writing phase
 */

@Slf4j
@Data
public class PaymentInstrumentWriterListener implements ItemWriteListener<InboundPaymentInstrument> {

    private String errorTransactionsLogsPath;
    private String executionDate;
    private Boolean enableOnErrorLogging;
    private Boolean enableOnErrorFileLogging;
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    @Override
    public void beforeWrite(List<? extends InboundPaymentInstrument> inboundPaymentInstruments) {}

    public void afterWrite(List<? extends InboundPaymentInstrument> inboundPaymentInstruments) {}

    public void onWriteError(Exception throwable, List<? extends InboundPaymentInstrument> inboundPaymentInstruments) {

        inboundPaymentInstruments.forEach(inboundPaymentInstrument -> {

            if (enableOnErrorLogging) {
                log.error("Error during PI record writing - {}, line: " +
                                "{}, filename: {}",
                        throwable.getMessage(),
                        inboundPaymentInstrument.getLineNumber(),
                        inboundPaymentInstrument.getFilename());
            }

            if (enableOnErrorFileLogging) {
                try {
                    String filename = inboundPaymentInstrument.getFilename().replaceAll("\\\\", "/");
                    String[] fileArr = filename.split("/");
                    File file = new File(
                            resolver.getResource(errorTransactionsLogsPath).getFile().getAbsolutePath()
                                    .concat("/".concat(executionDate))
                                    + "_WriteErrorRecords_"+fileArr[fileArr.length-1]
                                    .replaceAll(".csv","")
                                    .replaceAll(".pgp","")+".csv");
                    FileUtils.writeStringToFile(
                            file, buildCsv(inboundPaymentInstrument), Charset.defaultCharset(), true);

                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }

        });

    }

    public void onWriteError(Exception throwable, InboundPaymentInstrument inboundPaymentInstrument) {

        if (enableOnErrorLogging) {
            log.error("Error during PI record writing - {}, line: " +
                            "{}, filename: {}",
                    throwable.getMessage(),
                    inboundPaymentInstrument.getLineNumber(),
                    inboundPaymentInstrument.getFilename());
        }

        if (enableOnErrorFileLogging) {
            try {
                String filename = inboundPaymentInstrument.getFilename().replaceAll("\\\\", "/");
                String[] fileArr = filename.split("/");
                File file = new File(
                        resolver.getResource(errorTransactionsLogsPath).getFile().getAbsolutePath()
                                .concat("/".concat(executionDate))
                                + "_WriteErrorRecords_"+fileArr[fileArr.length-1]
                                .replaceAll(".csv","")
                                .replaceAll(".pgp","")+".csv");
                FileUtils.writeStringToFile(
                        file, buildCsv(inboundPaymentInstrument), Charset.defaultCharset(), true);

            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }

    }

    private String buildCsv(InboundPaymentInstrument inboundPaymentInstrument) {
        return (inboundPaymentInstrument.getFiscalCode() != null ? inboundPaymentInstrument.getFiscalCode() : "").concat(";")
                .concat(inboundPaymentInstrument.getHpan() != null ? inboundPaymentInstrument.getHpan() : "").concat(";")
                .concat(inboundPaymentInstrument.getCancellationDate() != null ? inboundPaymentInstrument.getCancellationDate() : "")
                .concat("\n");
    }

}
