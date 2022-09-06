package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class CallNumberToolsTest {


  @Test
  public void collectionFlags() throws IOException {
    Map<String,Character> callNumbers = new LinkedHashMap<>();
    callNumbers.put("New & Noteworthy Books AC8.5 .G74 2016", ' ');
    callNumbers.put("QA1", 'm');
    callNumbers.put("QA75.A1 Z632", 'e');
    callNumbers.put("QA155 .A33 1981", 'm');
    callNumbers.put("QA155.7.E4 B37 2015", ' ');
    callNumbers.put("QA", 'm');
    callNumbers.put("QA276", 'm');
    callNumbers.put("QA276.", 'm');
    callNumbers.put("QA276.4 .S587 2010eb", ' ');
    callNumbers.put("QA276.411", ' ');
    callNumbers.put("QA276.7", 'm');
    callNumbers.put("T123.5 R6", 'e');
    callNumbers.put("Oversize T123.5 R6", 'e');
    callNumbers.put("TX123.5 R6", ' ');
    for ( String call : callNumbers.keySet() ) {
      List<String> flags = CallNumberTools.getCollectionFlags(new HashSet<>(Arrays.asList(call)));
      switch (callNumbers.get(call)) {
      case ' ':
        assertTrue(flags.isEmpty());
        break;
      case 'm':
        assertFalse(flags.isEmpty());
        assertEquals("Math Library",flags.get(0));
        break;
      case 'e':
        assertFalse(flags.isEmpty());
        assertEquals("Engineering Library",flags.get(0));
        break;
      }
    }
    
  }
}
