package it.gov.pagopa.rtd.csv_connector.service;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Service
@Data
@RequiredArgsConstructor
@Slf4j
public class WriterTrackerServiceImpl implements WriterTrackerService {

    private final List<CountDownLatch> countDownLatches;

    @SneakyThrows
    @Override
    public synchronized void addCountDownLatch(
            CountDownLatch countDownLatch, Boolean enableCheckpointFrequency, Integer checkpointFrequency) {
        countDownLatches.add(countDownLatch);
        if (enableCheckpointFrequency && countDownLatches.size() % checkpointFrequency == 0) {
            countDownLatch.await();
        }
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
