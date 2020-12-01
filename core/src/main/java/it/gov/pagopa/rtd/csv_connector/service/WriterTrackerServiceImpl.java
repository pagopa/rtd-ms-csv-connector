package it.gov.pagopa.rtd.csv_connector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Service
@RequiredArgsConstructor
@Slf4j
public class WriterTrackerServiceImpl implements WriterTrackerService {

    private final List<CountDownLatch> countDownLatches;

    @Override
    public synchronized void addCountDownLatch(CountDownLatch countDownLatch) {
        countDownLatches.add(countDownLatch);
    }

    @Override
    public synchronized List<CountDownLatch> getCountDownLatches() {
        return countDownLatches;
    }

    @Override
    public void clearAll() {
        countDownLatches.clear();
    }
}
