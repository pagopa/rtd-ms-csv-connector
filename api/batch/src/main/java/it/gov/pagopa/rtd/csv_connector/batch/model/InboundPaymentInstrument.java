package it.gov.pagopa.rtd.csv_connector.batch.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class InboundPaymentInstrument {

    @NotBlank
    @NotNull
    private String fiscalCode;

    @NotBlank
    @NotNull
    private String hpan;

    @NotBlank
    @NotNull
    private String cancellationDate;

    Integer lineNumber;
    String filename;

}
