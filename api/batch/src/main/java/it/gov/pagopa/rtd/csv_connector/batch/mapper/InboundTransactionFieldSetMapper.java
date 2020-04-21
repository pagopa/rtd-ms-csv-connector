package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.time.OffsetDateTime;

public class InboundTransactionFieldSetMapper implements FieldSetMapper<InboundTransaction> {

    @Override
    public InboundTransaction mapFieldSet(FieldSet fieldSet) throws BindException {

        if (fieldSet == null) {
            return null;
        }

        InboundTransaction inboundTransaction =
                InboundTransaction.builder()
                        .acquirerCode(fieldSet.readString("codice_acquirer"))
                        .operationType(fieldSet.readString("tipo_operazione"))
                        .circuitType(fieldSet.readString("tipo_circuito"))
                        .pan(fieldSet.readString("PAN"))
                        .trxDate(OffsetDateTime.parse(fieldSet.readString("timestamp")))
                        .idTrxAcquirer(fieldSet.readInt("id_trx_acquirer"))
                        .idTrxIssuer(fieldSet.readInt("id_trx_issuer"))
                        .correlationId(fieldSet.readInt("correlation_id"))
                        .amount(fieldSet.readBigDecimal("importo"))
                        .amountCurrency(fieldSet.readString("currency"))
                        .acquirerId(fieldSet.readString("acquirerID"))
                        .merchantId(fieldSet.readString("merchantID"))
                        .mcc(fieldSet.readString("MCC"))
                        .build();

        return inboundTransaction;

    }

}
