package it.gov.pagopa.rtd.csv_connector.service;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public interface WriterTrackerService {

    void addCountDownLatch(
            CountDownLatch countDownLatch,
            Boolean enableCheckpointFrequency,
            String filename, Integer checkpointFrequency);

    List<CountDownLatch> getCountDownLatches();

    List<CountDownLatch> getFileCountDownLatches(String filename);


    void clearAll();

}
