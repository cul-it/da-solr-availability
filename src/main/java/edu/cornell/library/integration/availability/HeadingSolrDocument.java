package edu.cornell.library.integration.availability;

import org.apache.solr.client.solrj.beans.Field;

public class HeadingSolrDocument {

  @Field("id") final int headingId;

  public HeadingSolrDocument(int headingId) { this.headingId = headingId; }
}
