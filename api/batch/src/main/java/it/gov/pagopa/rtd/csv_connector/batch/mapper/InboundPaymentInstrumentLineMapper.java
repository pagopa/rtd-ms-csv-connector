package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.Data;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

@Data
public class InboundPaymentInstrumentLineMapper implements LineMapper<InboundPaymentInstrument>, InitializingBean {

    private LineTokenizer tokenizer;

    private FieldSetMapper<InboundPaymentInstrument> fieldSetMapper;

    private String filename;

    public InboundPaymentInstrument mapLine(String line, int lineNumber) throws Exception {
        try{
            InboundPaymentInstrument inboundPaymentInstrument = fieldSetMapper.mapFieldSet(tokenizer.tokenize(line));
            inboundPaymentInstrument.setLineNumber(lineNumber);
            inboundPaymentInstrument.setFilename(filename);
            return inboundPaymentInstrument;
        }
        catch(Exception ex){
            throw new FlatFileParseException("Parsing error at line: " + lineNumber, ex, line, lineNumber);
        }
    }

    public void afterPropertiesSet() {
        Assert.notNull(tokenizer, "The LineTokenizer must be set");
        Assert.notNull(fieldSetMapper, "The FieldSetMapper must be set");
    }

}