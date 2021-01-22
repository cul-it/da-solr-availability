package edu.cornell.library.integration.availability;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CallNumberTools {

  public static void loadPrefixes(String prefixFileName) throws IOException {
    if ( prefixes == null ) {
      List<String> p = new ArrayList<>();
      try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(prefixFileName);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in));){
        String line;
        while ( ( line = reader.readLine() ) != null ) {
          if (line.indexOf('#') != -1) line = line.substring(0,line.indexOf('#'));
          line = line.trim().toLowerCase();
          if (! line.isEmpty()) p.add(line);
        }
      }
      prefixes = p;
    }
  }

  static String generateClassification(Connection inventory, String callNum) throws SQLException, IOException {
    if (inventory == null) return null;
    if ( prefixes == null )
      loadPrefixes("callnumberprefixes.txt");

      Matcher m = lcClass.matcher(sortForm(callNum,prefixes));
    if (m.matches()) {
      if ( classificationQ == null )
        classificationQ = inventory.prepareStatement(
            "  select label from classifications.classification"
            + " where ? between low_letters and high_letters"
            + "   and ? between low_numbers and high_numbers"
            + " order by high_letters desc, high_numbers desc");
      classificationQ.setString(1, m.group(1));
      classificationQ.setString(2, m.group(2));

      try ( ResultSet rs = classificationQ.executeQuery() ) {
        List<String> parts = new ArrayList<>();
        while ( rs.next() )
          parts.add(rs.getString(1));
        return String.join(" > ", parts);
      }

    }
    return null;

  }
  private static Pattern lcClass = Pattern.compile("([a-z]{1,3}) ?([0-9\\.]{0,15}).*");
  private static PreparedStatement classificationQ = null;
  private static List<String> prefixes = null;
  //  private static Pattern lcClass = Pattern.compile("([A-Za-z]{1,3}) ?\\.?([0-9]{1,6})[^0-9]\\.*");

  public static String getNumberAfterFirstLetters( String callNumberSortForm ) {
    Matcher m = lcClass.matcher(callNumberSortForm);
    if ( m.matches() )
      return m.group(2);
    return null;
  }

  public static String sortForm( CharSequence callNumber ) throws IOException {
    if ( prefixes == null )
      loadPrefixes("callnumberprefixes.txt");
    return sortForm(callNumber,prefixes);
  }

  static String sortForm( CharSequence callNumber, List<String> prefixes ) {

    String lc = Normalizer.normalize(callNumber, Normalizer.Form.NFD)
        .toLowerCase().trim()
        // periods not followed by digits aren't decimals and must go
        .replaceAll("\\.(?!\\d)", " ").replaceAll("\\s+"," ");

    if (lc.isEmpty()) return lc;

    return stripPrefixes(lc,prefixes)
        // all remaining non-alphanumeric (incl decimals) must go
        .replaceAll("[^a-z\\d\\.]+", " ")
        // separate alphabetic and numeric sections
        .replaceAll("([a-z])(\\d)", "$1 $2")
        .replaceAll("(\\d)([a-z])", "$1 $2")
        // zero pad first integer number component if preceded by
        // not more than one alphabetic block
        .replaceAll("^\\s*([a-z]*\\s*)(\\d+)", "$100000000$2")
        .replaceAll("^([a-z]*\\s*)0*(\\d{9})", "$1$2")

        .trim();
  }


  private static String stripPrefixes(String callnum, List<String> prefixes) {

    if (prefixes == null || prefixes.isEmpty())
      return callnum;

    String previouscallnum;

    do {

      previouscallnum = callnum;
      PREF: for (String prefix : prefixes) {
        if (callnum.startsWith(prefix)) {
          callnum = callnum.substring(prefix.length());
          break PREF;
        }
      }

      while (callnum.length() > 0 && -1 < " ,;#+".indexOf(callnum.charAt(0))) {
        callnum = callnum.substring(1);
      }

    } while (callnum.length() > 0 && ! callnum.equals(previouscallnum)); 

    return callnum;
  }

  public static boolean hasMathCallNumber(Set<String> callNumbers) throws IOException {

    for ( String callNumber : callNumbers ) {
      String sortCall = CallNumberTools.sortForm(callNumber);
      if (! sortCall.startsWith("qa")) continue;
//    exclude cs ranges: QA 75-76, QA 155.7, QA 267-268, QA 402.3, QA 402.35, QA 402.37
      String number = CallNumberTools.getNumberAfterFirstLetters(sortCall);
      if (number == null || number.isEmpty()) return true; //include plain QA call numbers
      Integer i = null;
      String decimal = null;
      if (number.contains(".")) {
        int dotPos = number.indexOf('.');
        i = Integer.valueOf(number.substring(0, dotPos));
        decimal = number.substring(dotPos+1);
      } else
        i = Integer.valueOf(number);
      if ( i == 75 || i == 76 ) continue;
      if ( i == 155 && decimal != null && decimal.equals("7") ) continue;
      if ( i == 267 || i == 268 ) continue;
      if ( i == 402 && decimal != null &&
          ( decimal.equals("3") || decimal.equals("35") || decimal.equals("37") )) continue;
      return true;
    }
    return false;
  }

}
