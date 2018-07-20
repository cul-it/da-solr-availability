package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;

import java.util.List;

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
    assertEquals(
        "SolrInputDocument(fields: [location=Library Annex, id=123_1, bibid_i=123,"
        + " callnum_sort=ABC123 .DE45 123, callnum_display=ABC123 .DE45])",
        docs.get(0).toString());
    assertEquals(
        "SolrInputDocument(fields: [location=Library Annex, id=123_2, bibid_i=123,"
        + " callnum_sort=Oversize ABC123 .DE45 123, callnum_display=Oversize ABC123 .DE45])",
        docs.get(1).toString());
  }

  @Test
  public void noCallNumRecord() {
    SolrInputDocument mainDoc = new SolrInputDocument();
    mainDoc.addField("id", "456");
    mainDoc.addField("location", "Library Annex");
    List<SolrInputDocument> docs = CallNumberBrowse.generateBrowseDocuments(mainDoc);
    assertEquals(0,docs.size());
  }


}
