package it.gov.pagopa.rtd.csv_connector.batch.listener;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Implementation of {@link ItemProcessListener}, to be used to log and/or store records
 * filtered or that have produced an error during a record processing phase
 */
@Slf4j
@Data
public class PaymentInstrumentProcessListener implements ItemProcessListener<InboundPaymentInstrument, InboundPaymentInstrument> {

    private String errorTransactionsLogsPath;
    private String executionDate;
    private Boolean enableOnErrorLogging;
    private Boolean enableOnErrorFileLogging;
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    @Override
    public void beforeProcess(InboundPaymentInstrument item) {}

    @Override
    public void afterProcess(InboundPaymentInstrument item, InboundPaymentInstrument result) {}

    public void onProcessError(InboundPaymentInstrument item, Exception throwable) {

        if (enableOnErrorLogging) {
            log.error("Error during PI record writing - {}, line: " +
                            "{}, filename: {}",
                    throwable.getMessage(),
                    item.getLineNumber(),
                    item.getFilename());
        }

        if (enableOnErrorFileLogging) {
            try {
                String filename = item.getFilename().replaceAll("\\\\", "/");
                String[] fileArr = filename.split("/");
                File file = new File(
                        resolver.getResource(errorTransactionsLogsPath).getFile().getAbsolutePath()
                                .concat("/".concat(executionDate))
                                + "_ValidationErrorRecords_"+fileArr[fileArr.length-1]
                                .replaceAll(".csv","")
                                .replaceAll(".pgp","")+".csv");
                FileUtils.writeStringToFile(file, buildCsv(item), Charset.defaultCharset(), true);
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
