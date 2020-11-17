package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.mapper.PaymentInstrumentMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.service.PaymentInstrumentConnectorService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
@Data
@Component
public class PaymentInstrumentWriter implements ItemWriter<InboundPaymentInstrument> {

    private final PaymentInstrumentConnectorService paymentInstrumentConnectorService;
    private final PaymentInstrumentMapper paymentInstrumentMapper;
    private String timestampParser;

    @Override
    public void write(List<? extends InboundPaymentInstrument> inboundPaymentInstruments) throws Exception {
        paymentInstrumentConnectorService.disablePaymentInstrument(
                inboundPaymentInstruments.stream().map(inboundPaymentInstrument ->
                        paymentInstrumentMapper.map(inboundPaymentInstrument,timestampParser))
                        .collect(Collectors.toList()));
    }

}
