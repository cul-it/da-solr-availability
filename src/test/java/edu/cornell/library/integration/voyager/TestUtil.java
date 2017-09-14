package edu.cornell.library.integration.voyager;

public class TestUtil {

  public static String convertStreamToString(java.io.InputStream is) {
    String val;
      try ( java.util.Scanner s = new java.util.Scanner(is) ) {
        s.useDelimiter("\\A");
        val = s.hasNext() ? s.next() : "";
      }
      return val;
  }

  
}
