package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

public class CallNumberToolsTest {


  @Test
  public void areTheseMathCallNumbers() throws IOException {

    assertFalse(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("New & Noteworthy Books AC8.5 .G74 2016"))));
    assertTrue(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA1"))));
    assertFalse(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA75.A1 Z632"))));
    assertTrue(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA155 .A33 1981"))));
    assertFalse(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA155.7.E4 B37 2015"))));
    assertTrue(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA"))));

    assertTrue(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA276"))));
    assertTrue(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA276."))));
    assertFalse(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA276.4 .S587 2010eb"))));
    assertFalse(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA276.411"))));
    assertTrue(CallNumberTools.hasMathCallNumber(new HashSet<>(Arrays.asList("QA276.7"))));
}

}
