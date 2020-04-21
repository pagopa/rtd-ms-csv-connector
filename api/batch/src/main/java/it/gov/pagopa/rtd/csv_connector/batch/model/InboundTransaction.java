package it.gov.pagopa.rtd.csv_connector.batch.model;

import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"idTrxAcquirer", "acquirerCode", "trxDate"}, callSuper = false)
public class InboundTransaction {

    @NotNull
    Integer idTrxAcquirer;

    @NotNull
    @NotBlank
    @Size(max = 20)
    String acquirerCode;

    @NotNull
    @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
    OffsetDateTime trxDate;

    @Size(max = 64)
    String pan;

    @Size(max = 2)
    String operationType;

    @Size(max = 2)
    String circuitType;

    Integer idTrxIssuer;

    Integer correlationId;

    BigDecimal amount;

    @Size(max = 3)
    String amountCurrency;

    String mcc;

    String acquirerId;

    String merchantId;

}