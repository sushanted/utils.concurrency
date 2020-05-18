package sr.utils.concurrency.partitioned.control;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Restrict the number of concurrent executions for id to a fixed number
 *
 * @param <T>
 */
public class PartitionedConcurrencyManager<T> {

  private final Map<T, SlotRecord> slots = new ConcurrentHashMap<>();
  private final int concurrency;

  public PartitionedConcurrencyManager(final int concurrency) {
    this.concurrency = concurrency;
  }

  public void runWithManagedConcurrency(//
      final T id, //
      final Runnable runnable, //
      final Consumer<Runnable> runner//
  //
  ) {
    runner.accept(//
        getConcurrencyManagedRunnable(//
            id, //
            runnable, //
            () -> runWithManagedConcurrency(//
                id, //
                runnable, //
                runner//
            )//
        )//
    );
  }

  public Runnable getConcurrencyManagedRunnable(//
      final T id, //
      final Runnable runnable, //
      // A callback when a slot gets available
      final Runnable slotAvailabilityRunner//
  ) {

    return () -> runWrapped(//
        id, //
        runnable, //
        slotAvailabilityRunner//
    );

  }

  private void runWrapped(//
      final T id, //
      final Runnable runnable, //
      final Runnable slotAvailabilityRunner//
  ) {
    final SlotRecord slotRecord = this.slots.compute(//
        id, //
        (jid, record) -> SlotRecord.getSlot(record, this.concurrency)//
    );

    if (!slotRecord.isSlotAvailable()) {
      return;
    }

    try {
      runnable.run();
    } finally {
      processSlotAvailability(id, slotAvailabilityRunner);
    }
  }

  private void processSlotAvailability(//
      final T id, //
      final Runnable slotAvailabilityRunner//
  ) {
    final SlotRecord returnedSlotRecord = this.slots.compute(//
        id, //
        (jid, record) -> SlotRecord.returnSlot(record)//
    );

    if (returnedSlotRecord != null && returnedSlotRecord.isAnythingPending()) {
      slotAvailabilityRunner.run();
    }
  }

}
