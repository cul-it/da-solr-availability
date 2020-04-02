package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;

import org.junit.Test;

import edu.cornell.library.integration.changes.Change;

public class ProcessAvailabilityQueueTest {

  @Test
  public void appendElapsedTime() {

    StringBuilder sb = new StringBuilder();
    Change.appendElapsedTime(sb, new Timestamp(System.currentTimeMillis() - 5_000 )); // 5 seconds
    assertEquals( " (5 seconds)",sb.toString());

    sb.setLength(0);
    Change.appendElapsedTime(sb, new Timestamp(System.currentTimeMillis() - 277_200_456 )); // 3d,5hr,and change
    assertEquals( " (3 days, 5 hours)",sb.toString());

    sb.setLength(0);
    Change.appendElapsedTime(sb, new Timestamp(System.currentTimeMillis() - 259_620_000 )); // 3d,7 minutes
    assertEquals( " (3 days)",sb.toString());

    sb.setLength(0);
    Change.appendElapsedTime(sb, new Timestamp(System.currentTimeMillis() - 258_600_000 )); // 10m less than 3d
    assertEquals( " (2 days, 23 hours)",sb.toString());
  }
}
