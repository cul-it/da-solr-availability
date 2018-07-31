package edu.cornell.library.integration.solr;

import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.analysis.util.MultiTermAwareComponent;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Solr analysis filter for call number sort, which zero pads numbers so they
 * will sort numerically in a string field. Implementation of MultiTermAwareComponent allows
 * filter to work at query time in range queries.
 *
 */
public class CallNumberSortFilterFactory extends TokenFilterFactory implements MultiTermAwareComponent {

  public CallNumberSortFilterFactory(Map<String,String> map) {
    super(map);
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new CallNumberSortFilter(input);
  }

  @Override
  public AbstractAnalysisFactory getMultiTermComponent() {
    return this;
  }
}
