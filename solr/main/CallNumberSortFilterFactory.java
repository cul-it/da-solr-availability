package edu.cornell.library.integration.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.analysis.util.ResourceLoaderAware;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Constructs a {@link CallNumberSortFilter}.
 * <pre class="prettyprint">
 * &lt;fieldType name="callNumberSort" class="solr.TextField" sortMissingLast="true" omitNorm="true"&gt;
 *  &lt;analyzer&gt;
 *   &lt;tokenizer class="solr.KeywordTokenizerFactory"/&gt;
 *   &lt;filter class="edu.cornell.library.integration.solr.CallNumberSortFilterFactory" prefixes="callnumberprefixes.txt"/&gt;
 *  &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>

 * Solr analysis filter for call number sort, optimized for LC call numbers, but supporting
 * other call number formats as sort behaviors are identified, to the extent that the call
 * number formats can be appropriately distinguished from one another.
 * 
 * <h2>Call Number Prefixes</h2>
 * 
 * Call number prefixes, if provided, will be excluded from sort. Prefixes
 * typically correspond with MARC holdings 852‡k values, but the filter is only aware of whether the
 * call number begins with a value identified as a prefix. If not provided, no default prefixes
 * are assumed, though this could be changed in the future if an option to opt out is also offered.
 * A prefix file might look like this:
 * 
 * <pre>
 * ## Sample call number prefixes not to sort on ##
 * Oversize
 * Microfiche
 * New Books
 * Thesis</pre>
 * 
 * Note that because lines beginning with hash marks are interpreted as comments, it isn't possible
 * to include a prefix that itself begins with a hash mark. However, because the sort normalization
 * already discards the hash mark as irrelevant to sort, the prefix can be included without the hash
 * mark.<br/><br/>
 * 
 * Also, prefixes are compared with each supplied call number in the order they appear in the prefixes
 * file. So, if call one prefix extends another, the longer prefix should appear first. For example,
 * listing {@code Rare Books} before {@code Rare} can ensure that the longer prefix will match first,
 * where applicable.
 *
 */
public class CallNumberSortFilterFactory 
  extends TokenFilterFactory implements ResourceLoaderAware {

  private List<String> prefixes;
  private final String prefixFile;

  public CallNumberSortFilterFactory(Map<String,String> args) {
    super(args);
    this.prefixFile = get(args, "prefixes");
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new CallNumberSortFilter(input, this.prefixes);
  }

  @Override
  public TokenStream normalize(TokenStream input) {
    return create(input);
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (this.prefixFile != null) {
      List<String> temp = getLines(loader, this.prefixFile);
      this.prefixes = new ArrayList<>();
      for (String s : temp)
        this.prefixes.add(s.toLowerCase().replaceAll("\\.(?!\\d) *"," "));
    }
  }

  public List<String> getPrefixes() {
    return this.prefixes;
  }
}
