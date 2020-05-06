//*********************************************************************
//Copyright 2019 VMware, Inc.  All rights reserved. VMware Confidential
//*********************************************************************
package sr.utils.concurrency.partitioned.control;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restrict the number of concurrent executions for id to a fixed number
 *
 * @param <T>
 */
public class PartitionedConcurrencyManager<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionedConcurrencyManager.class);

  private final Map<T, SlotRecord> slots = new ConcurrentHashMap<>();
  private final int concurrency;

  public PartitionedConcurrencyManager(final int concurrency) {
    this.concurrency = concurrency;
  }

  public Runnable getRunnable(//
      final T id, //
      final Runnable runnable, //
      final Consumer<T> slotAvailabilityConsumer//
  ) {

    return () -> wrapRunnable(//
        id, //
        runnable, //
        slotAvailabilityConsumer//
    );

  }

  private void wrapRunnable(//
      final T id, //
      final Runnable runnable, //
      final Consumer<T> slotAvailabilityConsumer//
  ) {
    final SlotRecord slotRecord = this.slots.compute(//
        id, //
        (jid, record) -> SlotRecord.getSlot(record, this.concurrency)//
    );

    if (!slotRecord.isSlotAvailable()) {
      LOGGER.info("Slot not available for id: {} ", id);
      return;
    }

    LOGGER.debug("Got slot for id {}", id);

    try {
      runnable.run();
    } finally {
      processSlotAvailability(id, slotAvailabilityConsumer);
    }
  }

  private void processSlotAvailability(final T id, final Consumer<T> slotAvailabilityConsumer) {
    final SlotRecord returnedSlotRecord = this.slots.compute(//
        id, //
        (jid, record) -> SlotRecord.returnSlot(record)//
    );

    if (returnedSlotRecord != null && returnedSlotRecord.isAnythingPending()) {
      slotAvailabilityConsumer.accept(id);
    } else {
      // TODO check if these logging are required
      if (returnedSlotRecord == null) {
        LOGGER.info("Removing record for id {}", id);
      }

      LOGGER.info("All pending tasks got slots for id {}", id);
    }
  }

}
