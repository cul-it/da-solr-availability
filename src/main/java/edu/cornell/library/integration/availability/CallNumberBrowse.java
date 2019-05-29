package edu.cornell.library.integration.availability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.cornell.library.integration.voyager.Holding;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;

class CallNumberBrowse {

  private static final List<String> clonedDocFields = Arrays.asList(
      "format",
      "pub_date_facet",
      "language_facet",
      "acquired_dt",
      "fulltitle_display",
      "fulltitle_vern_display") ;

  private static final String callNumberField = "lc_callnum_full";
  private static final String urlField = "url_access_json";

  private static BibliographicSummary availableSummary;
  static {
    availableSummary = new BibliographicSummary();
    Map<String,String> availAt = new HashMap<>();
    availAt.put("Available for the Library to Purchase" , null);
    availableSummary.availAt = availAt;
  }

  static List<SolrInputDocument> generateBrowseDocuments(SolrInputDocument doc, HoldingSet holdings)
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
      boolean availableCallNum = callNum.equals("Available for the Library to Purchase");

      // try to pull bibliographic call number for missing call number
      if ( isNonCallNumber(callNum )) {
        String bibCallNumber = getBibCallNumber( doc.getFieldValues(callNumberField) );
        if (bibCallNumber != null && ! bibCallNumber.isEmpty()) {
          callNum = bibCallNumber;
          bibliographicCallNum = true;
        } else {
          continue; // Suppress from callnum browse when no discoverable callnum.
        }
      }

      BibliographicSummary b ;
      if ( availableCallNum )
        b = availableSummary;
      else
        b = BibliographicSummary.summarizeHoldingAvailability(holdingsForCallNum);

      if (b.online != null && b.online) {
        if ( doc.containsKey(urlField)) {
          browseDoc.addField("online", "Online");
        } else {
          // no online access without a link
          System.out.println("Serv,remo holdings, no access link. b"+bibId);
          if ( b.availAt != null || b.unavailAt != null)
            b.online = null; // if also print, suppress online from record
          else
            continue;        // otherwise, suppress call number from browse entirely
        }
      }
      if (b.availAt != null)   for (String loc : b.availAt.keySet())     b.availAt.put(loc, "");
      if (b.unavailAt != null) for (String loc : b.unavailAt.keySet())   b.unavailAt.put(loc, "");

      String id = bibId+"."+(++i);
      browseDoc.addField( "id", id );
      browseDoc.addField( "callnum_sort" , callNum+" 0 "+id );
      browseDoc.addField( "callnum_display" , callNum );
      browseDoc.addField( "availability_json", b.toJson() );

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

  private static String getBibCallNumber(Collection<Object> callNumbers) {
    if (callNumbers == null) return null;
    for (Object call : callNumbers)
      if ( ! isNonCallNumber( (String) call ))
        return (String) call;
    return null;
  }

  private static boolean isNonCallNumber( String call ) {
    String lc = call.toLowerCase().replaceAll("\\s+"," ");
    return lc.contains("no call") || lc.contains("in proc")
        || lc.contains("on order") || lc.startsWith("available ");
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



  public static Set<String> collateCallNumberList(List<SolrInputDocument> callNumberDocs) {
    Set<String> callNums = new HashSet<>();
    for (SolrInputDocument doc : callNumberDocs)
      callNums.add( (String) doc.getFieldValue("callnum_display") );
    return callNums;
  }

}
