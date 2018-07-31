package edu.cornell.library.integration.solr;

import java.io.IOException;
//import java.lang.invoke.MethodHandles;
import java.text.Normalizer;

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

  protected CallNumberSortFilter(TokenStream input) {
    super(input);
  }

  @Override
  public final boolean incrementToken() throws IOException {

    if (!input.incrementToken()) return false;

    String outputValue = callNumberSortForm(termAttr);
    termAttr.setEmpty();
    termAttr.append(outputValue);
    return true;
  }

  public static String callNumberSortForm( CharSequence callNumber ) {
    return Normalizer.normalize(callNumber, Normalizer.Form.NFD)
        .toLowerCase()

        // periods not followed by digits aren't decimals and must go
        .replaceAll("\\.(?!\\d)", " ")
        // all remaining non-alphanumeric (incl decimals) must go
        .replaceAll("[^a-z\\d\\.]+", " ")
        // separate alphabetic and numeric sections
        .replaceAll("([a-z])(\\d)", "$1 $2")
        .replaceAll("(\\d)([a-z])", "$1 $2")
        // zero pad integer number components (digits not preceded by periods)
        .replaceAll("(?<![\\.\\d])(\\d+)", "00000000$1")
        .replaceAll("(?<![\\.\\d])0*(\\d{9})", "$1")

        .trim();
  }
}
