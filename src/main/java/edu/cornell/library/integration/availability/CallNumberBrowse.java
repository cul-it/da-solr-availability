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

    String bibId = (String)doc.getFieldValue("id");
    Collection<Object> callNumbers = doc.getFieldValues(callNumberField);
    Collection<String> callNumberStrings = new HashSet<>();
    for (Object o : callNumbers) callNumberStrings.add((String)o);//dedupe

    int i = 0;
    for (String callNo : callNumberStrings) {
      SolrInputDocument browseDoc = callNoDoc.deepCopy();

      String id = bibId+"."+(++i);
      browseDoc.addField( "id", id );
      browseDoc.addField( "bibid_i", bibId );
      browseDoc.addField( "callnum_sort" , callNo+" "+id );
      browseDoc.addField( "callnum_display" , callNo );

      browseDocs.add(browseDoc);
    }

    return browseDocs;
  }

}
