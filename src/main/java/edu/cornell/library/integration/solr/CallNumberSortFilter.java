package edu.cornell.library.integration.solr;

import java.io.IOException;
//import java.lang.invoke.MethodHandles;
import java.text.Normalizer;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * Solr analysis filter for call number sort, which zero pads numbers so they
 * will sort numerically in a string field.
 *
 */
public final class CallNumberSortFilter extends TokenFilter {

//  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final List<String> prefixes;

  protected CallNumberSortFilter(TokenStream input, List<String> prefixes) {
    super(input);
    this.prefixes = Collections.unmodifiableList(prefixes);
  }

  @Override
  public final boolean incrementToken() throws IOException {

    if (!input.incrementToken()) return false;

    String outputValue = callNumberSortForm(termAttr,prefixes);
    termAttr.setEmpty();
    termAttr.append(outputValue);
    return true;
  }

  public static String callNumberSortForm( CharSequence callNumber, List<String> prefixes ) {

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
}
