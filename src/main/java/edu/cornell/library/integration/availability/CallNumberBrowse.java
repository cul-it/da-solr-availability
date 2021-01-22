package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.voyager.Holding;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;

class CallNumberBrowse {

  private static final List<String> clonedDocFields = Arrays.asList(
      "format",
      "pub_date_facet",
      "language_facet",
      "acquired_dt",
      "fulltitle_display",
      "fulltitle_vern_display",
      "pub_date_display",
      "isbn_display",
      "oclc_id_display") ;

  private static final String lcCallNumberField = "lc_bib_display";
  private static final String urlField = "url_access_json";

  private static BibliographicSummary availableSummary;
  static {
    Map<String,String> availAt = new HashMap<>();
    availAt.put("Available for the Library to Purchase" , null);
    availableSummary = new BibliographicSummary(null,null,availAt,null);
  }

  static List<SolrInputDocument> generateBrowseDocuments(
      Connection inventory, SolrInputDocument doc, HoldingSet holdings)
      throws SQLException, IOException {
    List<SolrInputDocument> browseDocs = new ArrayList<>();
    if ( holdings.getMfhdIds().isEmpty() ) return browseDocs;

    SolrInputDocument callNumDoc = new SolrInputDocument();
    for ( String field : clonedDocFields )
      if (doc.containsKey(field))
        callNumDoc.put(field, doc.getField(field));
    String bibId = (String)doc.getFieldValue("id");
    if ( doc.containsKey("publisher_display") )
      callNumDoc.addField("publisher_display",
          doc.getFieldValues("publisher_display").stream().map(f -> (String) f).collect(Collectors.joining(",")));
    if ( doc.containsKey("author_display") ) {
      String author = (String)doc.getFieldValue("author_display");
      if (author.endsWith(", author"))
        author = author.substring(0, author.lastIndexOf(','));
      else if (author.endsWith("- author"))
        author = author.substring(0, author.lastIndexOf(' '));
      callNumDoc.addField("author_display",author);
    }
    callNumDoc.addField("bibid", bibId);
    callNumDoc.addField("cite_preescaped_display", generateCitation(callNumDoc));

    String defaultCall = getBibCallNumber( doc.getFieldValues(lcCallNumberField) );
    System.out.println("bib call: "+defaultCall);
    if ( defaultCall == null || defaultCall.isEmpty() )
      defaultCall = selectDefaultHoldingCallNumber( holdings );
    System.out.println("default call: "+defaultCall);
    Map<String,HoldingSet> holdingsByCallNumber =
        divideUnsuppressedHoldingsByCallNumber( holdings, defaultCall );

    int i = 0;
    for ( Entry<String,HoldingSet> e : holdingsByCallNumber.entrySet() ) {
      String callNum = e.getKey();
      HoldingSet holdingsForCallNum = e.getValue();

      Holding h1 = holdingsForCallNum.values().iterator().next();
      boolean isLc = ( Objects.equals(callNum, h1.call) ) ? h1.lcCallNum : true ;
      SolrInputDocument browseDoc = callNumDoc.deepCopy();
      boolean availableCallNum = callNum.equals("Available for the Library to Purchase");

      BibliographicSummary b ;
      if ( availableCallNum ) {
        if ( defaultCall == null || defaultCall.isEmpty() ) continue;
        callNum = defaultCall;
        isLc = true;
        b = availableSummary;
      } else
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
      browseDoc.addField( "lc_b", isLc );
      browseDoc.addField( "availability_json", b.toJson() );
      if ( isLc ) {
        String classificationDescription = CallNumberTools.generateClassification( inventory, callNum );
        if ( classificationDescription != null && ! classificationDescription.isEmpty() )
          browseDoc.addField("classification_display", classificationDescription);
      }

      Set<String> locations = holdingsForCallNum.getLocationFacetValues();
      if (locations != null && ! locations.isEmpty()) {
        browseDoc.addField("location", locations );
        browseDoc.addField("online", "At the Library");
      }

      browseDocs.add(browseDoc);
    }
    return browseDocs;
  }


  private static String generateCitation(SolrInputDocument doc) {
    StringBuilder citation = new StringBuilder();
    if (doc.containsKey("author_display")) {
      appendEscaped(citation,(String) doc.getFieldValue("author_display"));
      if ( citation.length() > 0 ) {
        char lastChar = citation.charAt(citation.length()-1);
        citation.append((lastChar == '.' || lastChar == '-')?" ":". ");
      }
    }
    if (doc.containsKey("fulltitle_display") || doc.containsKey("fulltitle_vern_display")) {
      List<String> titles = new ArrayList<>();
      if (doc.containsKey("fulltitle_vern_display")) titles.add((String) doc.getFieldValue("fulltitle_vern_display"));
      if (doc.containsKey("fulltitle_display"))      titles.add((String) doc.getFieldValue("fulltitle_display"));
      citation.append("<strong>");
      appendEscaped(citation,String.join(" / ", titles));
      citation.append(".</strong> ");
    }
    if (doc.containsKey("publisher_display")) {
      appendEscaped(citation,(String) doc.getFieldValue("publisher_display"));
      citation.append(", ");
    }
    if (doc.containsKey("pub_date_display")) {
      citation.append((String) doc.getFieldValue("pub_date_display"));
      citation.append(".");
    }
    return citation.toString();
  }

  private static void appendEscaped(StringBuilder citation, String value) {
    for ( int i = 0; i < value.length(); i++ ) {
      char c = value.charAt(i);
      switch (c) {
      case '<':  citation.append("&lt;");   break;
      case '>':  citation.append("&gt;");   break;
      case '&':  citation.append("&amp;");  break;
      case '"':  citation.append("&quot;"); break;
      case '\'': citation.append("&#x27;"); break;
      case '/':  citation.append("&#x2F;"); break;
      default: citation.append(c);
      }
    }
  }

  private static String getBibCallNumber(Collection<Object> callNumbers) {
    if (callNumbers == null) return null;
    for (Object call : callNumbers)
      if ( ! isNonCallNumber( (String) call ))
        return (String) call;
    return null;
  }

  private static String selectDefaultHoldingCallNumber(HoldingSet holdings) {
    for (Holding h : holdings.values()) if ( h.lcCallNum && h.call != null && ! h.call.isEmpty() ) return h.call;
    return null;
  }

  private static boolean isNonCallNumber( String call ) {
    String lc = call.toLowerCase().replaceAll("\\s+"," ");
    return lc.contains("no call") || lc.contains("in proc") || lc.contains("on order") ;
  }

  private static Map<String, HoldingSet> divideUnsuppressedHoldingsByCallNumber(HoldingSet holdings, String defaultCall) {
    Map<String,HoldingSet> holdingsByCallNumber = new HashMap<>();
    for (Integer holdingId : holdings.getMfhdIds()) {
      Holding h = holdings.get(holdingId);
      if ( h.active == false ) continue;
      String callnum = h.call;

      boolean nonCallNumber = (callnum == null || callnum.isEmpty() || isNonCallNumber(callnum ) );

      if ( ! nonCallNumber ) {
        if ( ! holdingsByCallNumber.containsKey(callnum) )
          holdingsByCallNumber.put(callnum, new HoldingSet());
        holdingsByCallNumber.get(callnum).put(holdingId, h);
      }

      if ( ( nonCallNumber || (h.lcCallNum == false && isClosedStackLocation(h)) )
          && defaultCall != null && ! defaultCall.isEmpty() ) {
        if ( ! holdingsByCallNumber.containsKey(defaultCall) )
          holdingsByCallNumber.put(defaultCall, new HoldingSet());
        holdingsByCallNumber.get(defaultCall).put(holdingId, h);
      }

    }
    return holdingsByCallNumber;
  }



  private static boolean isClosedStackLocation(Holding h) {

    if ( h.location == null ) return false;
    if ( h.location.library != null && 
        ( h.location.library.equals("Kroch Library Rare & Manuscripts")
            || h.location.library.equals("ILR Library Kheel Center")
            || h.location.library.equals("Library Annex") ))
      return true;
    if ( h.location.name != null &&
        ( h.location.name.equals("Music Locked Press")
            || h.location.name.equals("Mann Special Collections") ))
      return true;
    return false;
  }

  public static Set<String> collateCallNumberList(List<SolrInputDocument> callNumberDocs) {
    Set<String> callNums = new HashSet<>();
    for (SolrInputDocument doc : callNumberDocs) {
      if ( (boolean)doc.getFieldValue("lc_b") ) {
        String callnum = (String) doc.getFieldValue("callnum_display");
        if ( ! lettersOnly.matcher(callnum).matches() ) callNums.add( callnum );
      }
    }
    return callNums;
  }
  private static Pattern lettersOnly = Pattern.compile("[A-Za-z]{1,3}");


}
