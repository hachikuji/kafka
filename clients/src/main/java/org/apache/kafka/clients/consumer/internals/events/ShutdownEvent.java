package org.apache.kafka.clients.consumer.internals.events;

public class ShutdownEvent extends CompletableApplicationEvent<Void> {
  public ShutdownEvent(long deadlineMs) {
    super(Type.SHUTDOWN, deadlineMs);
  }
}
