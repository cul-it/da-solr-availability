package edu.cornell.library.integration.availability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.voyager.Holding;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;

public class CallNumberBrowse {

  private static final List<String> clonedDocFields = Arrays.asList(
      "format",
      "author_facet",
      "pub_date_facet",
      "language_facet",
      "fast_topic_facet",
      "fast_geo_facet",
      "fast_era_facet",
      "fast_genre_facet",
      "subject_content_facet",
      "acquired_dt",
      "fulltitle_display",
      "fulltitle_vern_display",
      "author_display",
      "pub_date_display",
      "language_display",
      "title_uniform_display",
      "edition_display") ;

  private static final String callNumberField = "lc_callnum_full";
  private static final String urlField = "url_access_json";

  public static List<SolrInputDocument> generateBrowseDocuments(SolrInputDocument doc, HoldingSet holdings)
      throws JsonProcessingException {
    List<SolrInputDocument> browseDocs = new ArrayList<>();
    if ( holdings.getMfhdIds().isEmpty() ) return browseDocs;

    SolrInputDocument callNumDoc = new SolrInputDocument();
    for ( String field : clonedDocFields )
      if (doc.containsKey(field))
        callNumDoc.put(field, doc.getField(field));
    String bibId = (String)doc.getFieldValue("id");
    callNumDoc.addField("bibid", bibId);


    Map<String,HoldingSet> holdingsByCallNumber = divideHoldingsByCallNumber( holdings );

    int i = 0;
    for ( Entry<String,HoldingSet> e : holdingsByCallNumber.entrySet() ) {
      String callNum = e.getKey();
      HoldingSet holdingsForCallNum = e.getValue();

      SolrInputDocument browseDoc = callNumDoc.deepCopy();
      boolean bibliographicCallNum = false;

      // try to pull bibliographic call number for missing call number
      if ( callNum.equals("No Call Number") || 
          callNum.equalsIgnoreCase("In Process") || 
          callNum.equalsIgnoreCase("On Order")) {
        String bibCallNumber = (String) doc.getFieldValue(callNumberField);
        if (bibCallNumber != null && ! bibCallNumber.isEmpty()) {
          callNum = bibCallNumber;
          bibliographicCallNum = true;
        } else {
          continue; // Suppress from callnum browse when no discoverable callnum.
        }
      }

      // populate per-call number fields
      String id = bibId+"."+(++i);
      browseDoc.addField( "id", id );
      browseDoc.addField( "callnum_sort" , callNum+" 0 "+id );
      browseDoc.addField( "callnum_display" , callNum );

      BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(holdingsForCallNum);
      browseDoc.addField("availability_json", b.toJson());

      if (b.online != null && b.online) {
        browseDoc.put(urlField, doc.getField(urlField));
        browseDoc.addField("online", "Online");
      }

      Set<String> locations = holdingsForCallNum.getLocationFacetValues();
      if (locations != null && ! locations.isEmpty()) {
        browseDoc.addField("location", locations );
        browseDoc.addField("online", "At the Library");
        if ( ! bibliographicCallNum )
          browseDoc.addField("shelfloc", true);
      }
      if (bibliographicCallNum)
        browseDoc.addField("flag", "Bibliographic Call Number");

      browseDocs.add(browseDoc);
    }
    return browseDocs;
  }

  private static Map<String, HoldingSet> divideHoldingsByCallNumber(HoldingSet holdings) {
    Map<String,HoldingSet> holdingsByCallNumber = new HashMap<>();
    for (Integer holdingId : holdings.getMfhdIds()) {
      Holding h = holdings.get(holdingId);
      String callnum = h.call;
      if (callnum == null || callnum.isEmpty())
        callnum = "No Call Number";
      if ( ! holdingsByCallNumber.containsKey(callnum) )
        holdingsByCallNumber.put(callnum, new HoldingSet());
      holdingsByCallNumber.get(callnum).put(holdingId, h);
    }
    return holdingsByCallNumber;
  }

}
