package edu.cornell.library.integration.availability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

public class CallNumberBrowse {

  private static final List<String> clonedDocFields = Arrays.asList(
      "online",
      "format",
      "author_facet",
      "pub_date_facet",
      "language_facet",
      "fast_topic_facet",
      "fast_geo_facet",
      "fast_era_facet",
      "fast_genre_facet",
      "subject_content_facet",
      "location",
      "acquired_dt",
      "fulltitle_display",
      "fulltitle_vern_display",
      "author_display",
      "pub_date_display",
      "language_display",
      "availability_json",
      "url_access_json",
      "title_uniform_display",
      "edition_display") ;

  private static final String callNumberField = "lc_callnum_full";

  public static List<SolrInputDocument> generateBrowseDocuments(SolrInputDocument doc) {

    List<SolrInputDocument> browseDocs = new ArrayList<>();
    
    if ( ! doc.containsKey(callNumberField))
      return browseDocs;

    SolrInputDocument callNoDoc = new SolrInputDocument();
    for ( String field : clonedDocFields )
      if (doc.containsKey(field))
        callNoDoc.put(field, doc.getField(field));
    callNoDoc.addField("type", "callno");
    callNoDoc.addField("source", "unk");

    String bibId = (String)doc.getFieldValue("id");
    Collection<Object> callNumbers = doc.getFieldValues(callNumberField);
    Collection<String> callNumberStrings = new HashSet<>();
    for (Object c : callNumbers) callNumberStrings.add((String)c);

    int i = 0;
    for (String callNo : callNumberStrings) {
      if ( callNoDoc.containsKey("id") ) callNoDoc.remove("id");
      if ( callNoDoc.containsKey("bibid_i") ) callNoDoc.remove("bibid_i");
      if ( callNoDoc.containsKey("callnum_sort") ) callNoDoc.remove("callnum_sort");
      if ( callNoDoc.containsKey("callnum_display") ) callNoDoc.remove("callnum_display");

      callNoDoc.addField( "id", bibId+"_"+(++i) );
      callNoDoc.addField( "bibid_i", bibId );
      callNoDoc.addField( "callnum_sort" , callNo+" "+bibId );
      callNoDoc.addField( "callnum_display" , callNo );

      
      browseDocs.add(callNoDoc.deepCopy());
    }

    return browseDocs;
  }

}
