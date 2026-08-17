package edu.cornell.library.integration.solr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

public class SolrQueries {

  public static Map<String,Timestamp> getOldestSolrRecords(SolrClient solr, Instant cursor)
      throws SolrServerException, IOException {
  
    SolrQuery q = new SolrQuery();
    q.setFields("id","timestamp");
    q.setQuery("timestamp:[* TO \""+cursor+"\"]");
    q.setRows(1_000);
    q.setRequestHandler("standard");
    q.setSort("random",ORDER.asc);
    Map<String,Timestamp> recordIds = new HashMap<>();
    for (SolrDocument doc : solr.query(q)
        .getResults()) {
      Timestamp lastIndexDate = Timestamp.valueOf(((Date)doc
          .getFieldValue("timestamp")).toInstant().atZone(ZoneId.of("Z")).toLocalDateTime());
      recordIds.put((String)doc.getFieldValue("id"),lastIndexDate);
    }
    return recordIds;
  }

  public static Timestamp getMostRecentSolrTimestamp(SolrClient solr) throws SolrServerException, IOException {
    SolrQuery q = new SolrQuery();
    q.setFields("timestamp");
    q.setQuery("id:*");
    q.setRows(1);
    q.setSort("timestamp", ORDER.desc);
    q.setRequestHandler("standard");
    Timestamp mostRecentSolrTimestamp = null;
    for (SolrDocument doc : solr.query(q).getResults())
      mostRecentSolrTimestamp =
          Timestamp.valueOf(((Date)doc.getFieldValue("timestamp"))
              .toInstant().atZone(ZoneId.of("Z")).toLocalDateTime());
    return mostRecentSolrTimestamp;
  }

  /**
   * Determine, based on cached Solr documents, whether the blacklight solr document or call number solr document(s)
   * are changed from those last pushed to Solr. If they are changed, then update Solr and the record cache. Return
   * an indication of whether a Solr update was necessary.
   * @param solr - Solr client for the Blacklight Solr index
   * @param callnumSolr - Solr client for the call number Solr index
   * @param cacheDir - Local filepath for cached Solr documents
   * @param doc - SolrInputDocument prepared for the Blacklight Solr index
   * @param callnumDocs - SolrInputDocument(s) prepared for the call number Solr index
   * @return List<Boolean> of size 2, indicating whether the Blacklight and call number indexes were updated, respectively
   * @throws SolrServerException
   * @throws IOException
   */
  public static List<Boolean> updateInSolrBlacklightCallnum(SolrClient solr, SolrClient callnumSolr, String cacheDir,
      SolrInputDocument doc, Set<SolrInputDocument> callnumDocs) throws SolrServerException, IOException {

    String bibid = (String) doc.getFieldValue("id");
    String instance_id = (String) doc.getFieldValue("instance_id");
    Path fullCacheDir = Paths.get(cacheDir,instance_id.substring(0, 2),instance_id.substring(2, 4));
    Files.createDirectories(fullCacheDir);
    Path cacheFile = null;

    // MAIN RECORD LOGIC

    boolean saveToSolrB = true;
    String docXml = ClientUtils.toXML(doc);
    if (cacheDir != null) {
      cacheFile = Paths.get(fullCacheDir.toString(), bibid+"-blacklight.xml");
      if (Files.exists(cacheFile)) {
        String oldXml = Files.readString(cacheFile);
        if (docXml.equals(oldXml))
          saveToSolrB = false;
      }
    }
    if (saveToSolrB) {
      Files.writeString(cacheFile, docXml);
      solr.add(doc);
    }

    // CALLNUMBER LOGIC

    StringBuilder callNumXml = new StringBuilder();
    for (SolrInputDocument d : callnumDocs)
      callNumXml.append(ClientUtils.toXML(d)).append("\n");
    if (cacheDir != null) {
      cacheFile = Paths.get(fullCacheDir.toString(), bibid+"-callnumbers.xml");
      if (Files.exists(cacheFile)) {
        String oldXml = Files.readString(cacheFile);
        if (callNumXml.toString().equals(oldXml))
          return Arrays.asList(saveToSolrB, false);
      }
    }

    callnumSolr.deleteByQuery("bibid:"+bibid);
    if ( ! callnumDocs.isEmpty() )
      callnumSolr.add(callnumDocs);
    Files.writeString(cacheFile, callNumXml.toString());
    return Arrays.asList(saveToSolrB, true);

  }

}
