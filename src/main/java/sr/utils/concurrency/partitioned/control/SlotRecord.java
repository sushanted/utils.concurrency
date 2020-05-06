package sr.utils.concurrency.partitioned.control;

public class SlotRecord {

  private final int pendingCount;
  private final int availableSlots;
  private final int totalSlots;

  private final boolean slotAvailable;
  private final boolean anythingPending;

  public SlotRecord(//
      final int pendingCount, //
      final int availableSlots, //
      final int totalSlots, //
      final boolean slotAvailable, //
      final boolean anythingPending//
  ) {
    this.pendingCount = pendingCount;
    this.availableSlots = availableSlots;
    this.totalSlots = totalSlots;
    this.slotAvailable = slotAvailable;
    this.anythingPending = anythingPending;
  }

  public boolean isSlotAvailable() {
    return this.slotAvailable;
  }

  public boolean isAnythingPending() {
    return this.anythingPending;
  }

  public SlotRecord reserveSlot() {

    return this.availableSlots == 0 ? //
        new SlotRecord(//
            this.pendingCount + 1, // no slots available, one more pending
            this.availableSlots, //
            this.totalSlots, //
            false, // no slots available
            true// we just incremented the pending count
        ) : //
        new SlotRecord(//
            this.pendingCount, // slot available, no more pending
            this.availableSlots - 1, // giving out a slot
            this.totalSlots, //
            true, // slots available
            this.anythingPending// carry over the value
        );

  }

  public SlotRecord returnSlot() {

    return (this.pendingCount == 0 && this.availableSlots + 1 == this.totalSlots) ? //
    // No pending tasks and all running tasks have given back the slots
    // We are in the state of completeness
    // We have obtained everything we wanted in life
    // Now this is the time to give up our existence, for the new generations
    // null : Remove the entry from the map, and end our existence
        null
        : new SlotRecord(//
            // One pending task complete
            // if nothing was pending, it was a running task
            this.pendingCount == 0 ? 0 : this.pendingCount - 1, //
            // Got a slot back
            this.availableSlots + 1, //
            this.totalSlots, //
            true, // we just made one slot available
            this.pendingCount != 0// there are something pending tasks
        );
  }

  public static SlotRecord getSlot(final SlotRecord currentRecord, final int totalSlots) {

    return currentRecord == null ? //
        new SlotRecord(//
            0, // first ever slot assignment, nothing pending
            // totalSlots - 1 : because 1 slot is for new SlotRecord() exhausted
            totalSlots - 1, //
            totalSlots, //
            true, // this is the new slot available!
            false// first ever slot assignment, nothing pending before
        ) : //
        currentRecord.reserveSlot();
  }

  public static SlotRecord returnSlot(final SlotRecord slotRecord) {
    return slotRecord.returnSlot();
  }

}