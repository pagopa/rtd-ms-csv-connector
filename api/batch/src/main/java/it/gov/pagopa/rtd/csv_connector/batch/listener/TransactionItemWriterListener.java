package it.gov.pagopa.rtd.csv_connector.batch.listener;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

@Slf4j
@Data
public class TransactionItemWriterListener implements ItemWriteListener<InboundTransaction> {

    private String errorTransactionsLogsPath;
    private String executionDate;
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    @Override
    public void beforeWrite(List<? extends InboundTransaction> list) {

    }

    public void afterWrite(List<? extends InboundTransaction> inboundTransactions) {
        if (log.isDebugEnabled()) {
            inboundTransactions.forEach(inboundTransaction -> {
                log.debug("Transaction record from filename: "
                        + inboundTransaction.getFilename() + " ,line: "
                        + inboundTransaction.getLineNumber() +" written");
            });
        }
    }

    public void onWriteError(Exception throwable, List<? extends InboundTransaction> inboundTransactions) {

        inboundTransactions.forEach(inboundTransaction -> {

            if (log.isInfoEnabled()) {
                log.info("Error during during transaction record writing - " + throwable.getMessage() + ", filename: "
                        + inboundTransaction.getFilename() + ",line: " + inboundTransaction.getLineNumber());
            }

            try {
                File file = new File(
                        resolver.getResource(errorTransactionsLogsPath).getFile().getAbsolutePath()
                                .concat("/".concat(executionDate)) + "_transactionsErrorRecords.csv");
                FileUtils.writeStringToFile(
                        file, buildCsv(inboundTransaction), Charset.defaultCharset(), true);

            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error(e.getMessage(), e);
                }
            }

        });


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
