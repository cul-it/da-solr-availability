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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
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
      "pub_date_display") ;

  private static final String callNumberField = "lc_callnum_full";
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

    String bibCall = getBibCallNumber( doc.getFieldValues(callNumberField) );
    Map<String,HoldingSet> holdingsByCallNumber =
        divideUnsuppressedHoldingsByCallNumber( holdings, bibCall );

    int i = 0;
    for ( Entry<String,HoldingSet> e : holdingsByCallNumber.entrySet() ) {
      String callNum = e.getKey();
      HoldingSet holdingsForCallNum = e.getValue();

      SolrInputDocument browseDoc = callNumDoc.deepCopy();
      boolean availableCallNum = callNum.equals("Available for the Library to Purchase");

      BibliographicSummary b ;
      if ( availableCallNum ) {
        if ( bibCall == null || bibCall.isEmpty() ) continue;
        callNum = bibCall;
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
      browseDoc.addField( "availability_json", b.toJson() );
      String classificationDescription = generateClassification( inventory, callNum );
      if ( classificationDescription != null && ! classificationDescription.isEmpty() )
        browseDoc.addField("classification_display", classificationDescription);

      Set<String> locations = holdingsForCallNum.getLocationFacetValues();
      if (locations != null && ! locations.isEmpty()) {
        browseDoc.addField("location", locations );
        browseDoc.addField("online", "At the Library");
      }

      browseDocs.add(browseDoc);
    }
    return browseDocs;
  }

  private static String generateClassification(Connection inventory, String callNum) throws SQLException, IOException {
    if (inventory == null) return null;
    if ( prefixes == null ) {
      List<String> p = new ArrayList<>();
      try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("callnumberprefixes.txt");
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
  private static Pattern lcClass = Pattern.compile("([a-z]{1,3})([0-9\\.]{1,15}).*");
  private static PreparedStatement classificationQ = null;
  private static List<String> prefixes = null;
  //  private static Pattern lcClass = Pattern.compile("([A-Za-z]{1,3}) ?\\.?([0-9]{1,6})[^0-9]\\.*");

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
      citation.append("<b>");
      appendEscaped(citation,String.join(" / ", titles));
      citation.append(".</b> ");
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

  private static boolean isNonCallNumber( String call ) {
    String lc = call.toLowerCase().replaceAll("\\s+"," ");
    return lc.contains("no call") || lc.contains("in proc") || lc.contains("on order") ;
  }

  private static Map<String, HoldingSet> divideUnsuppressedHoldingsByCallNumber(HoldingSet holdings, String bibCall) {
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
          && bibCall != null && ! bibCall.isEmpty() ) {
        if ( ! holdingsByCallNumber.containsKey(bibCall) )
          holdingsByCallNumber.put(bibCall, new HoldingSet());
        holdingsByCallNumber.get(bibCall).put(holdingId, h);
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
      String callnum = (String) doc.getFieldValue("callnum_display");
      if ( ! lettersOnly.matcher(callnum).matches() ) callNums.add( callnum );
    }
    return callNums;
  }
  private static Pattern lettersOnly = Pattern.compile("[A-Za-z]{1,3}");

  private static String sortForm( CharSequence callNumber, List<String> prefixes ) {

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
