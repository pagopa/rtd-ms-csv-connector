package it.gov.pagopa.rtd.csv_connector.batch.model;

import lombok.*;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

/**
 * Model for the processed lines in the batch
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"idTrxAcquirer", "acquirerCode", "trxDate"}, callSuper = false)
public class InboundTransaction {

    @NotNull
    @NotBlank
    String idTrxAcquirer;

    @NotNull
    @NotBlank
    @Size(max = 20)
    String acquirerCode;

    @NotBlank
    @NotNull
    String trxDate;

    @NotNull
    @NotBlank
    String pan;

    @NotNull
    @NotBlank
    @Size(min = 2, max = 2)
    @Pattern(regexp = "[0-9]{2}")
    String operationType;

    @NotNull
    @NotBlank
    @Size(min = 2, max = 2)
    @Pattern(regexp = "[0-9]{2}")
    String circuitType;

    String idTrxIssuer;

    String correlationId;

    @NotNull
    Long amount;

    @Size(max = 3)
    String amountCurrency;

    @NotNull
    @NotBlank
    String mcc;

    String acquirerId;

    @NotNull
    @NotBlank
    String merchantId;

    @NotNull
    @NotBlank
    String terminalId;

    @NotNull
    @Pattern(regexp = "[0-9]{6}|[0-9]{8}")
    String bin;

    Integer lineNumber;
    String filename;

}