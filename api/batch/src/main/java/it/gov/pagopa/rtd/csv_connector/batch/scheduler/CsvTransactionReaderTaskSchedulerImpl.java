package it.gov.pagopa.rtd.csv_connector.batch.scheduler;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
class CsvTransactionReaderTaskSchedulerImpl extends ThreadPoolTaskScheduler
        implements CsvTransactionReaderTaskScheduler {

    private final Map<Object, ScheduledFuture<?>> scheduledTasks = new IdentityHashMap<>();

    private static final long serialVersionUID = 3276751893429922827L;

    public void cancelFutureSchedulerTasks() {
        scheduledTasks.forEach((k, v) -> {
            if (k instanceof CsvTransactionReaderTaskScheduler) {
                v.cancel(false);
            }
        });

    }


}
