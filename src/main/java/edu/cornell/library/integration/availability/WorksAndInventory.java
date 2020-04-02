package edu.cornell.library.integration.availability;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WorksAndInventory {

  private final static String selectBRS = "SELECT * FROM bibRecsSolr WHERE bib_id = ?";
  private final static String replaceBRS =
      "REPLACE INTO bibRecsSolr"+
      " (bib_id, record_date, linking_mod_date, title, format, pub_date, language, edition, online, print, active)"+
      " VALUES (?, ?, NOW(), ?, ?, ?, ?, ?, ?, ?, ?)" ;
  private final static String updateBRS = "UPDATE bibRecsSolr SET record_date = ? WHERE bib_id = ?";
  private final static String selectB2W = "SELECT * FROM bib2work WHERE bib_id = ? AND active = 1";
  private final static String selectB2W2 =
      "SELECT bib2work.bib_id "+
      "  FROM bib2work, solrFieldsData, bibRecsSolr"+
      " WHERE work_id = ?"+
      "   AND bib2work.bib_id = solrFieldsData.bib_id"+
      "   AND recordtype_solr_fields LIKE '%type: Catalog%'"+
      "   AND bib2work.bib_id = bibRecsSolr.bib_id"+
      "   AND bibRecsSolr.active = 1";
  private final static String insertB2W = "REPLACE INTO bib2work ( bib_id, oclc_id, work_id) VALUES (?,?,?)";
  private final static String selectW2O = "SELECT oclc_id, work_id from workids.work2oclc WHERE oclc_id = ?";
  private final static String updateB2W =
      "UPDATE bib2work SET active = 0"+
      " WHERE bib_id = ? AND oclc_id = ? AND work_id = ?";
  private final static String insertAvailQ = "INSERT INTO availabilityQueue (bib_id, priority, cause, record_date) VALUES (?,?,?,NOW())";
  private final static String selectMRS = "SELECT mfhd_id, record_date FROM mfhdRecsSolr WHERE bib_id = ?";
  private final static String updateMRS = "REPLACE INTO mfhdRecsSolr (bib_id, mfhd_id, record_date) VALUES (?,?,?)";
  private final static String deleteMRS = "DELETE FROM mfhdRecsSolr WHERE bib_id = ? AND mfhd_id = ?";
  private static Map<String,PreparedStatement> pstmts = new HashMap<>();

  public static void updateInventory(Connection inventory, SolrInputDocument doc)
      throws SQLException, JsonParseException, JsonMappingException, IOException {

    int bibId = Integer.valueOf( ((String) doc.getFieldValue("id")).replaceAll("[^\\d]", "") );
    Versions recordDates = getRecordDates( doc );

    // Get old and new linking metadata (metadata needed for "other forms" links
    if (! pstmts.containsKey("selectBRS")) pstmts.put("selectBRS", inventory.prepareStatement(selectBRS));
    pstmts.get("selectBRS").setInt(1,bibId);

    boolean newBib;
    LinkingMetadata oldMeta = null;
    try (ResultSet rs = pstmts.get("selectBRS").executeQuery()) {
      newBib = ! rs.next();
      if ( ! newBib )
        oldMeta = getOldMetadata( rs );
    }

    LinkingMetadata meta = getCurrentMetadata( doc );
    boolean linkingUpdate = newBib || ! meta.equals(oldMeta);


    // Get old and new OCLC work associations
    if (! pstmts.containsKey("selectB2W")) pstmts.put("selectB2W", inventory.prepareStatement(selectB2W));
    pstmts.get("selectB2W").setInt(1,bibId);

    Set<WorkLink> oldWorks = new HashSet<>();
    try (ResultSet rs = pstmts.get("selectB2W").executeQuery()) {
      while (rs.next())
        oldWorks.add(new WorkLink(rs.getLong("oclc_id"),rs.getLong("work_id")));
    }

    Set<WorkLink> works = getWorks( doc, inventory );
    boolean worksUpdate = ! works.equals(oldWorks);


    // Make necessary updates
    if (linkingUpdate)
      pushLinkingUpdate( meta, bibId, recordDates.bib, inventory );
    else
      updateIndexDate( bibId, recordDates.bib, inventory );

    if ( worksUpdate )
      pushWorksChanges( bibId, works, oldWorks, inventory );

    insertWorksFieldstoSolrDoc( bibId, works, doc, inventory );
    updateHoldingsInInventory( bibId, recordDates, inventory );
    triggerOtherWorksReindex( bibId, works, oldWorks, linkingUpdate, inventory );
  }

  public static void deleteWorkRelationships ( Connection inventory, List<Integer> bibIds ) throws SQLException {
    try ( PreparedStatement getWorkLinkedBibs = inventory.prepareStatement
            ("SELECT o.bib_id FROM bib2work as t, bib2work as o"+
             " WHERE t.bib_id = ?"+
             "   AND t.work_id = o.work_id"+
             "   AND t.bib_id != o.bib_id"+
             "   AND o.active = 1" );
        PreparedStatement deactiveB2W = inventory.prepareStatement("UPDATE bib2work SET active = 0 WHERE bib_id = ?") ){

      for (Integer bibId : bibIds) {
        Set<Integer> connectedBibs = new HashSet<>();
        getWorkLinkedBibs.setInt(1, bibId);
        try ( ResultSet rs = getWorkLinkedBibs.executeQuery() ) {
          while ( rs.next() ) {
            connectedBibs.add( rs.getInt(1) );
          }
        }
        deactiveB2W.setInt(1, bibId);
        deactiveB2W.addBatch();
        insertConnectedBibsToAvailQueue( connectedBibs, bibId, inventory );
      }
      deactiveB2W.executeBatch();
    }
    
  }

  private static void updateHoldingsInInventory(int bibId, Versions recordDates, Connection inventory) throws SQLException {
    if (! pstmts.containsKey("selectMRS")) pstmts.put("selectMRS", inventory.prepareStatement(selectMRS));

    Map<Integer,Timestamp> previousHoldings = new HashMap<>();
    pstmts.get("selectMRS").setInt(1, bibId);
    try ( ResultSet rs = pstmts.get("selectMRS").executeQuery()) {
      while (rs.next())
        previousHoldings.put(rs.getInt(1), rs.getTimestamp(2));
    }

    if (recordDates.holdings != null)
      for ( Entry<Integer, Timestamp> e : recordDates.holdings.entrySet()) {
        Integer mfhdId = e.getKey();
        boolean update = false;
        if ( previousHoldings.containsKey(mfhdId) ) {
          if ( previousHoldings.get(mfhdId).before(e.getValue()) )
            update = true;
          previousHoldings.remove(mfhdId);
        } else {
          update = true;
        }
  
        if ( ! update ) continue;
  
        if (! pstmts.containsKey("updateMRS")) pstmts.put("updateMRS", inventory.prepareStatement(updateMRS));
        pstmts.get("updateMRS").setInt(1, bibId);
        pstmts.get("updateMRS").setInt(2, mfhdId);
        pstmts.get("updateMRS").setTimestamp(3, e.getValue());
        pstmts.get("updateMRS").executeUpdate();
      }

    if ( previousHoldings.isEmpty()) return;
    if (! pstmts.containsKey("deleteMRS")) pstmts.put("deleteMRS", inventory.prepareStatement(deleteMRS));
    pstmts.get("deleteMRS").setInt(1, bibId);
    for (Integer mfhdId : previousHoldings.keySet()) {
      pstmts.get("deleteMRS").setInt(2, mfhdId);
      pstmts.get("deleteMRS").addBatch();
    }
    pstmts.get("deleteMRS").executeBatch();
  }

  private static void insertWorksFieldstoSolrDoc( Integer thisBibId, Set<WorkLink> works, SolrInputDocument doc, Connection inventory )
      throws SQLException, JsonGenerationException, JsonMappingException, IOException {

    if (works.isEmpty()) return;

    Set<Integer> bibIds = getBibsForWorks( works, inventory );
    bibIds.remove(thisBibId);
    if (bibIds.isEmpty()) return;

    for( Integer bibId : bibIds ) {
      pstmts.get("selectBRS").setInt(1,bibId);
      try ( ResultSet rs = pstmts.get("selectBRS").executeQuery() ) {
        while (rs.next()) {
          Map<String,Object> json = new HashMap<>();
          json.put("bibid", bibId);
          String s = rs.getString("title");    if (s != null) json.put("title", s);
                 s = rs.getString("format");   if (s != null) json.put("format", s);
                 s = rs.getString("pub_date"); if (s != null) json.put("pub_date", s);
                 s = rs.getString("language"); if (s != null) json.put("language", s);
                 s = rs.getString("edition");  if (s != null) json.put("edition", s);
          if ( rs.getBoolean("online") ) json.put("sites", true);
          if ( rs.getBoolean("print") )  json.put("libraries", true);
          ByteArrayOutputStream jsonstream = new ByteArrayOutputStream();
          mapper.writeValue(jsonstream, json);
          doc.addField("other_availability_json", jsonstream.toString());
        }
      }
    }
    for (WorkLink work : works) {
      doc.addField("workid_facet", work.workId);
      doc.addField("workid_display", work.workId);
    }
  }

  private static void triggerOtherWorksReindex(
      int bibId, Set<WorkLink> works, Set<WorkLink> oldWorks, boolean linkingUpdate, Connection inventory) throws SQLException {

    // if updates to linking, trigger all works.
    if (linkingUpdate)
      works.addAll(oldWorks);

    // else, trigger only removed and added works.
    else {
      Set<WorkLink> intersection = new HashSet<>(works);
      intersection.retainAll(oldWorks);
      works.removeAll(intersection);
      oldWorks.removeAll(intersection);
      works.addAll(oldWorks);
    }

    if (works.isEmpty()) return;

    // identify bibs for selected works
    Set<Integer> bibs = getBibsForWorks( works, inventory );
    bibs.remove(bibId);

    if (bibs.isEmpty()) return;

    insertConnectedBibsToAvailQueue( bibs, bibId, inventory );
  }

  private static void insertConnectedBibsToAvailQueue( Set<Integer> bibIds, int origBib, Connection inventory) throws SQLException {
    if (! pstmts.containsKey("insertAvailQ")) pstmts.put("insertAvailQ", inventory.prepareStatement(insertAvailQ));
    pstmts.get("insertAvailQ").setInt(2, 7);
    pstmts.get("insertAvailQ").setString(3, "Title Link from b"+origBib);
    for ( Integer bib : bibIds ) {
      pstmts.get("insertAvailQ").setInt(1, bib);
      pstmts.get("insertAvailQ").addBatch();
    }
    pstmts.get("insertAvailQ").executeBatch();

  }

  private static Set<Integer> getBibsForWorks(Set<WorkLink> works, Connection inventory) throws SQLException {
    Set<Integer> bibs = new HashSet<>();
    if (! pstmts.containsKey("selectB2W2")) pstmts.put("selectB2W2", inventory.prepareStatement(selectB2W2));
    for ( WorkLink w : works ) {
      pstmts.get("selectB2W2").setLong(1, w.workId);
      try ( ResultSet rs = pstmts.get("selectB2W2").executeQuery() ) {
        while (rs.next())
          bibs.add(rs.getInt(1));
      }
    }
    return bibs;
  }

  private static void pushWorksChanges(int bibId, Set<WorkLink> works, Set<WorkLink> oldWorks, Connection inventory) throws SQLException {

    // De-activate old works associations
    for ( WorkLink w : oldWorks )
      if ( ! works.contains(w)) {
        if (! pstmts.containsKey("updateB2W")) pstmts.put("updateB2W", inventory.prepareStatement(updateB2W));
        pstmts.get("updateB2W").setInt(1, bibId);
        pstmts.get("updateB2W").setLong(2, w.oclcId);
        pstmts.get("updateB2W").setLong(3, w.workId);
        pstmts.get("updateB2W").executeUpdate();
      }

    // Add new works associations
    for ( WorkLink w : works )
      if ( ! oldWorks.contains(w)) {
        if (! pstmts.containsKey("insertB2W")) pstmts.put("insertB2W", inventory.prepareStatement(insertB2W));
        pstmts.get("insertB2W").setInt(1, bibId);
        pstmts.get("insertB2W").setLong(2, w.oclcId);
        pstmts.get("insertB2W").setLong(3, w.workId);
        pstmts.get("insertB2W").executeUpdate();
      }
  }

  private static void updateIndexDate(int bibId, Timestamp recordDate, Connection inventory) throws SQLException {

    if (! pstmts.containsKey("updateBRS")) pstmts.put("updateBRS", inventory.prepareStatement(updateBRS));
    pstmts.get("updateBRS").setTimestamp(1, recordDate);
    pstmts.get("updateBRS").setInt(2, bibId);
    pstmts.get("updateBRS").executeUpdate();
  }

  private static void pushLinkingUpdate(LinkingMetadata meta, int bibId, Timestamp recordDate, Connection inventory) throws SQLException {

    if (! pstmts.containsKey("replaceBRS")) pstmts.put("replaceBRS", inventory.prepareStatement(replaceBRS));
    @SuppressWarnings("resource")
    PreparedStatement p = pstmts.get("replaceBRS");
    p.setInt(1, bibId);
    p.setTimestamp(2, recordDate);
    p.setString(3, meta.title);
    p.setString(4, meta.format);
    p.setString(5, meta.pubDate);
    p.setString(6, meta.language);
    p.setString(7, meta.edition);
    p.setBoolean(8, meta.online);
    p.setBoolean(9, meta.print);
    p.setBoolean(10, meta.active);
    p.executeUpdate();
  }

  private static Set<WorkLink> getWorks(SolrInputDocument doc, Connection inventory) throws SQLException {
    Set<WorkLink> works = new HashSet<>();
    if ( ! doc.containsKey("oclc_id_display") ) return works;

    if (! pstmts.containsKey("selectW2O")) pstmts.put("selectW2O", inventory.prepareStatement(selectW2O));
    Collection<Object> oclcIds = doc.getFieldValues("oclc_id_display");
    for ( Object oclcIdObject : oclcIds ) {
      String oclc = ((String)oclcIdObject).replaceAll("[^0-9]","");
      if ( oclc.isEmpty() || oclc.length() > 18 ) continue;
      Long oclcId = Long.valueOf(oclc);
      pstmts.get("selectW2O").setLong(1,oclcId);
      try (ResultSet rs = pstmts.get("selectW2O").executeQuery()) {
        while (rs.next())
          works.add(new WorkLink(oclcId,rs.getLong("work_id")));
      }
    }

    return works;
  }

  private static LinkingMetadata getOldMetadata(ResultSet rs) throws SQLException {
    LinkingMetadata meta = new LinkingMetadata();

    meta.title    = rs.getString("title");
    meta.format   = rs.getString("format");
    meta.pubDate  = rs.getString("pub_date");
    meta.language = rs.getString("language");
    meta.edition  = rs.getString("edition");
    meta.online   = rs.getBoolean("online");
    meta.print    = rs.getBoolean("print");
    meta.active   = rs.getBoolean("active");

    return meta;
  }

  private static LinkingMetadata getCurrentMetadata(SolrInputDocument doc) {
    LinkingMetadata meta = new LinkingMetadata();

    if ( doc.containsKey("title_uniform_display") ) {
      meta.title = (String) doc.getFieldValue("title_uniform_display");
      meta.title = meta.title.substring(0,meta.title.indexOf('|'));
    } else if ( doc.containsKey("title_vern_display" ) )
      meta.title = (String) doc.getFieldValue("title_vern_display");
    else if ( doc.containsKey("title_display" ) )
      meta.title = (String) doc.getFieldValue("title_display");

    if ( doc.containsKey("edition_display"))
      meta.edition  = (String) doc.getFieldValue("edition_display");

    if ( doc.containsKey("format"))
      meta.format   = doc.getFieldValues("format").stream().map(f -> (String) f).collect(Collectors.joining(","));
    if ( doc.containsKey("pub_date"))
      meta.pubDate  = doc.getFieldValues("pub_date").stream().map(f -> (String) f).collect(Collectors.joining(","));
    if ( doc.containsKey("language_facet"))
      meta.language = doc.getFieldValues("language_facet").stream().map(f -> (String) f).collect(Collectors.joining(","));

    if (doc.containsKey("online")) {
      Collection<Object> online = doc.getFieldValues("online");
      if (online.contains("Online"))
        meta.online = true;
      if (online.contains("At the Library"))
        meta.print = true;
    }
    if ( ((String)doc.getFieldValue("type")).equals("Suppressed Bib") )
      meta.active = false;

    return meta;
  }

  private static Versions getRecordDates(SolrInputDocument doc) throws JsonParseException, JsonMappingException, IOException {
    if ( ! doc.containsKey("record_dates_display") )
      return new Versions(null);
    return mapper.readValue((String)doc.getFieldValue("record_dates_display"), Versions.class);
  }

  private static class WorkLink implements Comparable<WorkLink> {
    final Long oclcId;
    final Long workId;
    WorkLink(Long oclcId, Long workId) {
      this.oclcId = oclcId;
      this.workId = workId;
    }
    @Override
    public boolean equals ( final Object o ) {
      if (this == o) return true;
      if (o == null) return false;
      if ( ! this.getClass().equals(o.getClass())) return false;
      WorkLink other = (WorkLink) o;
      return Objects.equals(this.oclcId, other.oclcId)
          && Objects.equals(this.workId, other.workId);
    }
    @Override
    public int hashCode() { return Long.hashCode( this.workId ); }
    @Override
    public int compareTo( final WorkLink o ) {
      int a = Long.compare(this.oclcId, o.oclcId);
      if ( a == 0 )
        return Long.compare(this.workId, o.workId);
      return a;
    }
  }

  protected static class LinkingMetadata {
    String title = null;
    String format = null;
    String pubDate = null;
    String language = null;
    String edition = null;
    boolean online = false;
    boolean print = false;
    boolean active = true;

    @Override
    public boolean equals ( final Object o ) {
      if (this == o) return true;
      if (o == null) return false;
      if ( ! this.getClass().equals(o.getClass())) return false;
      LinkingMetadata other = (LinkingMetadata) o;
      return Objects.equals(this.title, other.title)
          && Objects.equals(this.format, other.format)
          && Objects.equals(this.pubDate, other.pubDate)
          && Objects.equals(this.language, other.language)
          && Objects.equals(this.edition, other.edition)
          && Objects.equals(this.online, other.online)
          && Objects.equals(this.print, other.print)
          && Objects.equals(this.active, other.active);
    }

    @Override
    public int hashCode() {
      return String.format("t%s f%s d%s l%s e%s o%b p%b a%b",
          this.title,this.format,this.pubDate,this.language,this.edition,this.online,this.print,this.active).hashCode();
    }

  }

  @JsonAutoDetect(fieldVisibility = Visibility.ANY)
  private static class Versions {
    @JsonProperty("bib")      Timestamp bib;
    @JsonProperty("holdings") Map<Integer,Timestamp> holdings;
    public Versions ( Timestamp bibTime ) {
      this.bib = bibTime;
    }
    @JsonCreator
    public Versions (
        @JsonProperty("bib")      Timestamp bib,
        @JsonProperty("holdings") Map<Integer,Timestamp> holdings ) {
      this.bib = bib;
      this.holdings = holdings;
    }
  }

  static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.setSerializationInclusion(Include.NON_NULL);
  }

}
