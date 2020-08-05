package it.gov.pagopa.rtd.csv_connector.batch.listener;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.lang.Nullable;

import java.io.File;
import java.nio.charset.Charset;

@Slf4j
@Data
public class TransactionItemProcessListener implements ItemProcessListener<InboundTransaction,InboundTransaction> {

    private String errorTransactionsLogsPath;
    private String executionDate;
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    @Override
    public void beforeProcess(InboundTransaction inboundTransaction) {

    }

    public void afterProcess(InboundTransaction item, @Nullable InboundTransaction result) {

        if (result == null) {

            if (log.isInfoEnabled()) {
                    log.info("Filtered transaction record on filename: "
                            + item.getFilename() + " ,line: " +
                            item.getLineNumber());
            }

            try {
                File file = new File(
                        resolver.getResource(errorTransactionsLogsPath).getFile().getAbsolutePath()
                                .concat("/".concat(executionDate)) + "_transactionsFilteredRecords.csv");
                FileUtils.writeStringToFile(file, buildCsv(item), Charset.defaultCharset());
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error(e.getMessage(), e);
                }
            }

        } else {
            if (log.isDebugEnabled()) {
                log.debug("Processed transaction record on filename: " + item.getFilename() + " ,line: " +
                        item.getLineNumber());
            }
        }

    }

    public void onProcessError(InboundTransaction item, Exception throwable) {

        if (log.isInfoEnabled()) {
                log.info("Error during during transaction processing, filename: " +
                        item.getFilename() + ",line: " + item.getLineNumber());
        }

        try {
            File file = new File(
                    resolver.getResource(errorTransactionsLogsPath).getFile().getAbsolutePath()
                            .concat("/".concat(executionDate)) + "_transactionsErrorRecords.csv");
            FileUtils.writeStringToFile(file,buildCsv(item) , Charset.defaultCharset(), true);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }

    }

    private String buildCsv(InboundTransaction inboundTransaction) {
        return inboundTransaction.getAcquirerCode().concat(";")
                .concat(inboundTransaction.getOperationType()).concat(";")
                .concat(inboundTransaction.getCircuitType()).concat(";")
                .concat(inboundTransaction.getPan()).concat(";")
                .concat(inboundTransaction.getTrxDate().toString()).concat(";")
                .concat(inboundTransaction.getIdTrxAcquirer()).concat(";")
                .concat(inboundTransaction.getIdTrxIssuer()).concat(";")
                .concat(inboundTransaction.getCorrelationId()).concat(";")
                .concat(inboundTransaction.getAmount().toString()).concat(";")
                .concat(inboundTransaction.getAmountCurrency()).concat(";")
                .concat(inboundTransaction.getAcquirerId()).concat(";")
                .concat(inboundTransaction.getMerchantId()).concat(";")
                .concat(inboundTransaction.getTerminalId()).concat(";")
                .concat(inboundTransaction.getBin()).concat(";")
                .concat(inboundTransaction.getMcc()).concat("\n");
    }

}
