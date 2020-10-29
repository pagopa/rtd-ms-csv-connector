package it.gov.pagopa.rtd.csv_connector.batch.encryption.exception;

/**
 * Custom {@link Throwable} used to define errors in the decrypt phase of the reader
 */
public class PGPDecryptException extends Throwable {

    public PGPDecryptException() {
        super();
    }

    public PGPDecryptException(String message, Throwable cause) {
        super(message, cause);
    }

}
