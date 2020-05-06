//*********************************************************************
//Copyright 2019 VMware, Inc.  All rights reserved. VMware Confidential
//*********************************************************************
package sr.utils.concurrency.partitioned.control;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Test;

/**
 *
 * A sample invocation
 *
 */
public class PartitionedConcurrencyManagerTests {

  // TODO : test with multiple jobIds
  // TODO : test with higher number of invocations than the executor service
  // threads
  // TODO : test with 1 invocation
  // TODO : test with same invocations as executor service

  @Test
  public void test() throws Exception {

    final ThreadPoolExecutor executorService = new ThreadPoolExecutor(//
        5, //
        5, //
        5l, //
        TimeUnit.MINUTES, //
        new LinkedBlockingQueue<>()//
    );

    final AtomicInteger counter = new AtomicInteger();

    final PartitionedConcurrencyManager<Long> concurrencyManager = new PartitionedConcurrencyManager<>(3);

    IntStream.range(0, 10)//
        .map(i -> counter.incrementAndGet())//
        .mapToLong(Long::valueOf)//
        .mapToObj(longId -> getRunnable(counter, concurrencyManager, 99l, executorService))//
        .forEach(executorService::submit);

    System.in.read();

  }

  private Runnable getRunnable(//
      final AtomicInteger counter, //
      final PartitionedConcurrencyManager<Long> concurrencyManager, //
      final Long id, //
      final ExecutorService executorService) {

    return concurrencyManager.getRunnable(//
        (long) id, //
        () -> runTask(id, counter.incrementAndGet()), //
        // It is meant to be recursive for chaining purpose
        jobId -> executorService.submit(//
            getRunnable(//
                counter, //
                concurrencyManager, //
                jobId, //
                executorService)//
        )//
    );
  }

  public void runTask(//
      final long jobId, //
      final int invocationNumber//
  ) {

    IntStream.range(0, 5)//
        .map(i -> sleep(200))//
        .forEach(//
            i -> System.out.println("Running job with id " + jobId + " for invocation number " + invocationNumber)//
        );

    System.out.println("Task complete");
  }

  public static int sleep(final int millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    return millis;
  }

}
