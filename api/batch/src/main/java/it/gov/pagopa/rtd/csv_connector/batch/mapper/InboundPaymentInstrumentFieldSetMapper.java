package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.lang.Nullable;
import org.springframework.validation.BindException;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Implementation of {@link FieldSetMapper}, to be used for a reader
 * related to files containing {@link InboundTransaction} data
 */

@RequiredArgsConstructor
public class InboundPaymentInstrumentFieldSetMapper implements FieldSetMapper<InboundPaymentInstrument> {

    private final String timestampParser;

    /**
     *
     * @param fieldSet
     *          instance of FieldSet containing fields related to an {@link InboundTransaction}
     * @return instance of  {@link InboundTransaction}, mapped from a FieldSet
     * @throws BindException
     */
    @Override
    public InboundPaymentInstrument mapFieldSet(@Nullable FieldSet fieldSet) throws BindException {

        if (fieldSet == null) {
            return null;
        }

        DateTimeFormatter dtf = timestampParser != null && !timestampParser.isEmpty() ?
                DateTimeFormatter.ofPattern(timestampParser).withZone(ZoneId.systemDefault()): null;

        InboundPaymentInstrument inboundPaymentInstrument =
                InboundPaymentInstrument.builder()
                        .fiscalCode(fieldSet.readString("fiscal_code"))
                        .hpan(fieldSet.readString("hpan"))
                        .build();

        OffsetDateTime dateTime = dtf != null ?
                ZonedDateTime.parse(fieldSet.readString("timestamp"), dtf).toOffsetDateTime() :
                OffsetDateTime.parse(fieldSet.readString("timestamp"));

        if (dateTime != null) {
            inboundPaymentInstrument.setCancellationDate(fieldSet.readString("timestamp"));
        }

        return inboundPaymentInstrument;

    }

}
