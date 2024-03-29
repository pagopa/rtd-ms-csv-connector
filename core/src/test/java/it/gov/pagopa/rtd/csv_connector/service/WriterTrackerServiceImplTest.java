package it.gov.pagopa.rtd.csv_connector.service;

import eu.sia.meda.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class WriterTrackerServiceImplTest extends BaseTest {

    @Test
    public void testWriter() {
        ArrayList<CountDownLatch> countDownLatchArrayList = new ArrayList<>();
        HashMap<String, List<CountDownLatch>> fileMap = new HashMap<>();
        WriterTrackerServiceImpl writerTrackerService = new WriterTrackerServiceImpl(countDownLatchArrayList, fileMap);
        writerTrackerService.addCountDownLatch(
                new CountDownLatch(1),false, "test", 1);
        Assert.assertEquals(1,writerTrackerService.getCountDownLatches().size());
        writerTrackerService.clearAll();
        Assert.assertEquals(0,writerTrackerService.getCountDownLatches().size());
    }

}