package edu.cornell.library.integration.availability;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Properties;
import java.util.TreeMap;

import javax.xml.stream.XMLStreamException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;

import edu.cornell.library.integration.marc.ControlField;
import edu.cornell.library.integration.marc.DataField;
import edu.cornell.library.integration.marc.MarcRecord;
import edu.cornell.library.integration.marc.MarcRecord.RecordType;
import edu.cornell.library.integration.marc.Subfield;
import edu.cornell.library.integration.voyager.DownloadMARC;

public class SurveyOnlineHoldingsCallNumberPopulationRate {

  public static void main(String[] args) throws IOException, SQLException, XMLStreamException, NumberFormatException, SolrServerException {

    Properties prop = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties")){
      prop.load(in);
    }
    System.out.println(prop.getProperty("voyagerDBUrl"));
    System.out.println(prop.getProperty("voyagerDBUser"));
    System.out.println(prop.getProperty("voyagerDBPass"));
    List<String> prefixes = new ArrayList<>();
    try (
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("callnumberprefixes.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in))){
      String line;
      while ( ( line = reader.readLine() ) != null ) {
        if (line.indexOf('#') != -1) line = line.substring(0,line.indexOf('#'));
        line = line.trim().toLowerCase();
        if (! line.isEmpty()) prefixes.add(line);
      }
    }

    int cursor = 340001;
    int batchSize = 10_000;
    int maxId = 0;

    int no852Count = 0;
    int no852BCount = 0;
    int no004Count = 0;
    int onlineCount = 0;
    int lcCallCount = 0;
    int nonLcCallCount = 0;
    int noHoldingCallCount = 0;
    int bibCallCount = 0;
    int noBibCallCount = 0;
    try (

        Connection voyager = DriverManager.getConnection(
        prop.getProperty("voyagerDBUrl"),prop.getProperty("voyagerDBUser"),prop.getProperty("voyagerDBPass"));
        PreparedStatement holdingsStmt = voyager.prepareStatement(
            "SELECT bmf.mfhd_id, record_segment"+
            "  FROM bib_master bma, bib_mfhd bmf, mfhd_master mm, mfhd_data md"+
            " WHERE bma.suppress_in_opac = 'N'"+
            "   AND bma.bib_id = bmf.bib_id"+
            "   AND bmf.mfhd_id BETWEEN ? AND ?"+
            "   AND bmf.mfhd_id = mm.mfhd_id"+
            "   AND mm.suppress_in_opac = 'N'"+
            "   AND bmf.mfhd_id = md.mfhd_id"+
            " ORDER BY bmf.mfhd_id, md.seqnum");
        Statement stmt = voyager.createStatement();
        ResultSet maxIdRs = stmt.executeQuery("SELECT MAX(mfhd_id) FROM mfhd_master");
        HttpSolrClient solr = new HttpSolrClient(prop.getProperty("blacklightSolrUrl"))) {


      // Check Hathi ETAS titles
      SolrQuery q = new SolrQuery();
      q.setFields("id");
      q.setRows(2_000_000);
      q.setRequestHandler("search");
      q.setQuery("etas_facet:*");
      q.setFacet(false);

      int processedTitleCount = 0;
      TITLE: for (SolrDocument doc : solr.query(q).getResults()) {
        int bibId = Integer.valueOf((String)doc.getFieldValue("id"));

        if ( processedTitleCount % 10 == 0)
          System.out.printf("%d hathi recs done. %d haves, %d have nots.\n", processedTitleCount,bibCallCount,noBibCallCount);
        processedTitleCount++;
        MarcRecord bibRec = new MarcRecord(RecordType.HOLDINGS,
            DownloadMARC.downloadMrc(voyager, RecordType.BIBLIOGRAPHIC, bibId));
        for ( DataField f : bibRec.dataFields ) if ( f.tag.equals("050") || f.tag.equals("950") ){
          Matcher m = lcClass.matcher(CallNumberBrowse.sortForm(f.concatenateSpecificSubfields("ab"),prefixes));
          if (m.matches()) {
            bibCallCount++;
            continue TITLE;
          }
          System.out.printf("non lc call number in bib %d: %s\n",bibId,f.concatenateSpecificSubfields("ab"));
        }
        noBibCallCount++;
      }

      // Check native catalog online holdings records

      holdingsStmt.setFetchSize(2*batchSize);
      while (maxIdRs.next()) maxId = maxIdRs.getInt(1);
      System.out.printf("max mfhd id: %d\n", maxId);
      while ( cursor < maxId ) {

        System.out.printf("%d to %d\n",cursor,cursor+batchSize-1);
        holdingsStmt.setInt(1, cursor);
        holdingsStmt.setInt(2, cursor+batchSize-1);
        cursor += batchSize;

        Map<Integer,ByteArrayOutputStream> mfhdStreams = new TreeMap<>();
        try ( ResultSet mfhdMrc = holdingsStmt.executeQuery() ) {
          while ( mfhdMrc.next() ) {
            int mfhdId = mfhdMrc.getInt(1);
            if ( ! mfhdStreams.containsKey(mfhdId) ) mfhdStreams.put(mfhdId, new ByteArrayOutputStream());
            mfhdStreams.get(mfhdId).write(mfhdMrc.getBytes(2));
          }
        }

        MFHD: for ( Entry<Integer,ByteArrayOutputStream> e : mfhdStreams.entrySet() ) {
          e.getValue().close();
          MarcRecord rec = new MarcRecord(RecordType.HOLDINGS,
              new String(e.getValue().toByteArray(), StandardCharsets.UTF_8));
          DataField f852 = null;
          for ( DataField f : rec.dataFields ) if ( f.tag.equals("852") ) f852 = f;
          if (f852 == null) {
            System.out.printf("No f852 in mfhd %d\n",e.getKey());
            no852Count++;
            continue;
          }
          Subfield sfB = null;
          for ( Subfield sf : f852.subfields ) if ( sf.code.equals('b') ) sfB = sf;
          if (sfB == null) {
            System.out.printf("No f852$b in mfhd %d\n",e.getKey());
            no852BCount++;
            continue;
          }
          if ( ! sfB.value.trim().equals("serv,remo") ) continue;
          onlineCount++;
          String callNumber = f852.concatenateSpecificSubfields("hi");
          if ( callNumber == null || callNumber.isEmpty() ) {
            noHoldingCallCount++;
          } else if ( f852.ind1.equals('0') ) {
            lcCallCount++;
            continue;
          } else {
            nonLcCallCount++;
          }

          // If we get to this point, our online holding has either no call or a non-LC call
          int bibId = 0;
          for (ControlField cf : rec.controlFields) if (cf.tag.equals("004")) bibId = Integer.valueOf(cf.value);
          if (bibId == 0) {
            System.out.printf("No 004 in mfhd %d\n",bibId);
            no004Count++;
            continue;
          }
          MarcRecord bibRec = new MarcRecord(RecordType.HOLDINGS,
              DownloadMARC.downloadMrc(voyager, RecordType.BIBLIOGRAPHIC, bibId));
          for ( DataField f : bibRec.dataFields ) if ( f.tag.equals("050") || f.tag.equals("950") ){
            Matcher m = lcClass.matcher(CallNumberBrowse.sortForm(f.concatenateSpecificSubfields("ab"),prefixes));
            if (m.matches()) {
              System.out.printf("lc call number in bib %d for mfhd: %d: %s\n",bibId,e.getKey(),f.concatenateSpecificSubfields("ab"));
              bibCallCount++;
              continue MFHD;
            }
            System.out.printf("non lc call number in bib %d for mfhd: %d: %s\n",bibId,e.getKey(),f.concatenateSpecificSubfields("ab"));
          }
          noBibCallCount++;
        }

        System.out.printf("Running totals: no852Count: %d; no852BCount: %d; no004Count: %d; onlineCount: %d;"
            + " lcCallCount: %d; nonLcCallCount: %d; noHoldingCallCount: %d; bibCallCount: %d; noBibCallCount: %d\n",
            no852Count,no852BCount,no004Count,onlineCount,
            lcCallCount,nonLcCallCount,noHoldingCallCount,bibCallCount,noBibCallCount);
      }

      
    }

  }
  private static Pattern lcClass = Pattern.compile("([a-z]{1,3}) ([0-9\\.]{1,15}).*");
}
