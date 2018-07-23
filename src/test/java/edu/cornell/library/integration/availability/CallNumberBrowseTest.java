package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class CallNumberBrowseTest {

  @Test
  public void smallRecord() {
    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "123");
    mainDoc.addField("lc_callnum_full", "ABC123 .DE45");
    mainDoc.addField("lc_callnum_full", "Oversize ABC123 .DE45");
    mainDoc.addField("location", "Library Annex");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(mainDoc);

    assertEquals(2,docs.size());
    assertEquals("<doc boost=\"1.0\">"
        + "<field name=\"location\">Library Annex</field>"
        + "<field name=\"id\">123.1</field>"
        + "<field name=\"bibid_i\">123</field>"
        + "<field name=\"callnum_sort\">oversize abc 000123.000000 de 000045.000000 000123.100000</field>"
        + "<field name=\"callnum_display\">Oversize ABC123 .DE45</field></doc>",ClientUtils.toXML(docs.get(0)));
    assertEquals("<doc boost=\"1.0\">"
        + "<field name=\"location\">Library Annex</field>"
        + "<field name=\"id\">123.2</field>"
        + "<field name=\"bibid_i\">123</field>"
        + "<field name=\"callnum_sort\">abc 000123.000000 de 000045.000000 000123.200000</field>"
        + "<field name=\"callnum_display\">ABC123 .DE45</field></doc>",ClientUtils.toXML(docs.get(1)));
  }

  @Test
  public void noCallNumRecord() {
    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "456");
    mainDoc.addField("location", "Library Annex");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(mainDoc);
    assertEquals(0,docs.size());
  }

  @Test
  public void callNumberSortAnalysis() {
    assertEquals("ml 000410.000000 m 000619.000000 g 000056.000000 002012.000000",
        CallNumberBrowse.callNumberSortAnalysis("ML410.M619 G56 2012"));
    assertEquals("a 000123.000000 0000012345678.100000",
        CallNumberBrowse.callNumberSortAnalysis("A123 12345678.1"));
  }

}
