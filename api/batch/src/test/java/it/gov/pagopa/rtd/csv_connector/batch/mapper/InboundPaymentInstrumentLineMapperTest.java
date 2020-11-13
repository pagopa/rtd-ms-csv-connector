package it.gov.pagopa.rtd.csv_connector.batch.mapper;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundPaymentInstrument;
import it.gov.pagopa.rtd.csv_connector.batch.model.InboundTransaction;
import lombok.SneakyThrows;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

public class InboundPaymentInstrumentLineMapperTest {

    public InboundPaymentInstrumentLineMapperTest(){
        MockitoAnnotations.initMocks(this);
    }

    @BeforeClass
    public static void configTest() {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        ((Logger) LoggerFactory.getLogger("eu.sia")).setLevel(Level.DEBUG);
    }

    private InboundPaymentInstrumentLineMapper lineAwareMapper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() {
        lineAwareMapper = new InboundPaymentInstrumentLineMapper();
        lineAwareMapper.setFilename("test.csv");
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(";");
        delimitedLineTokenizer.setNames(
                "fiscal_code", "hpan", "timestamp");
        lineAwareMapper.setTokenizer(delimitedLineTokenizer);
        lineAwareMapper.setFieldSetMapper(new InboundPaymentInstrumentFieldSetMapper(
                "MM/dd/yyyy HH:mm:ss"));
    }

    @Test
    public void testMapper() {

        try {
            InboundPaymentInstrument inboundPaymentInstrument = lineAwareMapper.mapLine(
                    "13131;pan1;03/20/2020 10:50:33", 1);
            Assert.assertEquals(getInboundPaymentInstrument(), inboundPaymentInstrument);
            Assert.assertEquals((Integer) 1, inboundPaymentInstrument.getLineNumber());
            Assert.assertEquals("test.csv", inboundPaymentInstrument.getFilename());
        } catch (Exception exception) {
            exception.printStackTrace();
            Assert.fail();
        }

    }

    @SneakyThrows
    @Test
    public void testMapper_KO() {

        expectedException.expect(FlatFileParseException.class);
        lineAwareMapper.mapLine(
                "13131;pan1;03/20/2020T10:50:33", 1);

    }

    public InboundPaymentInstrument getInboundPaymentInstrument() {
        return InboundPaymentInstrument.builder()
                .fiscalCode("13131")
                .hpan("pan1")
                .cancellationDate("03/20/2020 10:50:33")
                .filename("test.csv")
                .lineNumber(1)
                .build();
    }

}