package it.gov.pagopa.rtd.csv_connector.integration.event.model;

import lombok.*;

import java.math.BigDecimal;

/**
 * Model for transaction to be sent in the outbound channel
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"idTrxAcquirer", "acquirerCode", "trxDate"}, callSuper = false)
public class Transaction {

    String idTrxAcquirer;

    String acquirerCode;

    String trxDate;

    String hpan;

    String operationType;

    String circuitType;

    String idTrxIssuer;

    String correlationId;

    BigDecimal amount;

    String amountCurrency;

    String mcc;

    String acquirerId;

    String merchantId;

    String terminalId;

    String bin;

    String par;

}