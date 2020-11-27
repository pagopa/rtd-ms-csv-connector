package it.gov.pagopa.rtd.csv_connector.batch.step;

import it.gov.pagopa.rtd.csv_connector.batch.listener.PaymentInstrumentWriterListener;
import it.gov.pagopa.rtd.csv_connector.batch.listener.TransactionItemWriterListener;
import it.gov.pagopa.rtd.csv_connector.batch.mapper.PaymentInstrumentMapper;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.connector.payment_instrument.model.PaymentInstrumentData;
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
    private PaymentInstrumentWriterListener paymentInstrumentWriterListener;
    private String timestampParser;

    @Override
    public void write(List<? extends InboundPaymentInstrument> inboundPaymentInstruments) {
        for (InboundPaymentInstrument inboundPaymentInstrument : inboundPaymentInstruments) {
            try {
                PaymentInstrumentData paymentInstrumentData =
                        paymentInstrumentMapper.map(inboundPaymentInstrument,timestampParser);
                paymentInstrumentConnectorService.disablePaymentInstrument(paymentInstrumentData);
            } catch (Exception e) {
                paymentInstrumentWriterListener.onWriteError(e, inboundPaymentInstrument);
                throw e;
            }
        }
    }

}
