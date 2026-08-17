package edu.cornell.library.integration.solr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
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

  public static void updateInSolrBlacklightCallnum(SolrClient solr, SolrClient callnumSolr, String cacheDir,
      SolrInputDocument doc, Set<SolrInputDocument> callnumDocs) throws SolrServerException, IOException {

    String bibid = (String) doc.getFieldValue("id");
    String instance_id = (String) doc.getFieldValue("instance_id");
    Path cacheFile = null;

    // MAIN RECORD LOGIC

    boolean saveToSolrB = true;
    String docXml = ClientUtils.toXML(doc);
    if (cacheDir != null) {
      cacheFile = Paths.get(cacheDir,instance_id.substring(0, 3), bibid+"-blacklight.xml");
      if (Files.exists(cacheFile)) {
        String oldXml = Files.readString(cacheFile);
        if (docXml.equals(oldXml))
          saveToSolrB = false;
      }
    }
    if (saveToSolrB) {
      Files.createDirectories(Paths.get(cacheDir,instance_id.substring(0, 3)));
      Files.writeString(cacheFile, docXml);
      solr.add(doc);
    }

    // CALLNUMBER LOGIC

    StringBuilder callNumXml = new StringBuilder();
    for (SolrInputDocument d : callnumDocs)
      callNumXml.append(ClientUtils.toXML(d)).append("\n");
    if (cacheDir != null) {
      cacheFile = Paths.get(cacheDir,instance_id.substring(0, 3), bibid+"-callnumbers.xml");
      if (Files.exists(cacheFile)) {
        String oldXml = Files.readString(cacheFile);
        if (callNumXml.toString().equals(oldXml))
          return;
      }
    }

    callnumSolr.deleteByQuery("bibid:"+bibid);
    if ( ! callnumDocs.isEmpty() )
      callnumSolr.add(callnumDocs);
    if ( ! saveToSolrB )
      Files.createDirectories(Paths.get(cacheDir,instance_id.substring(0, 3)));
    Files.writeString(cacheFile, callNumXml.toString());

  }

}
