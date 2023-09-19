package edu.cornell.library.integration.db_test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

public class TestUtil {
  public static String loadResourceFile(String filename) throws IOException {
    try ( InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
        Scanner s = new Scanner(is, "UTF-8") ) {
      return s.useDelimiter("\\A").next();
    }
  }
}
