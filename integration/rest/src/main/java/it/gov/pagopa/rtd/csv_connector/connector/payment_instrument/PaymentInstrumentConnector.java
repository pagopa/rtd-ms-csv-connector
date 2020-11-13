package it.gov.pagopa.rtd.csv_connector.connector.payment_instrument;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.validation.constraints.NotBlank;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * PaymentInstrumentRequest Rest Client
 */
@FeignClient(name = "${rest-client.payment-instrument.serviceCode}", url = "${rest-client.payment-instrument.base-url}")
public interface PaymentInstrumentConnector {

    @DeleteMapping(value = "${rest-client.payment-instrument.delete.url}",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody
    void deleteByFiscalCode(@NotBlank @PathVariable("hpan") String hpan,
                            @RequestParam("fiscalCode") String fiscalCode,
                            @RequestParam("cancellationDate") OffsetDateTime cancellationDate,
                            @RequestHeader("Ocp-Apim-Subscription-Key") String token);

}
