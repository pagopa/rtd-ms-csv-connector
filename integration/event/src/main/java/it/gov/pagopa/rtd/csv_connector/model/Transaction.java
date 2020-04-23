package it.gov.pagopa.rtd.csv_connector.model;

import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"idTrxAcquirer", "acquirerCode", "trxDate"}, callSuper = false)
public class Transaction {

    Integer idTrxAcquirer;

    String acquirerCode;

    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    OffsetDateTime trxDate;

    String hpan;

    String operationType;

    String circuitType;

    Integer idTrxIssuer;

    Integer correlationId;

    BigDecimal amount;

    String amountCurrency;

    String mcc;

    Integer acquirerId;

    Integer merchantId;

}