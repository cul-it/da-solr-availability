package edu.cornell.library.integration.solr;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrUtils {


  public static Http2SolrClient getSolrClient( Properties prop, String corePropName ) {

    return new Http2SolrClient
        .Builder(prop.getProperty("solrUrl")+"/"+prop.getProperty(corePropName))
        .withBasicAuthCredentials(prop.getProperty("solrUser"),prop.getProperty("solrPassword")).build();
    }


  public static Http2SolrClient getSolrClient( String solrUrl, String solrCore, String solrUser, String solrPassword ) {

    return new Http2SolrClient.Builder(solrUrl+"/"+solrCore).withBasicAuthCredentials(solrUser,solrPassword).build();
  }


  public static SolrDocument getdocumentById(SolrClient solr, String hrid) throws SolrServerException, IOException {
    return solr.getById(hrid);
  }

  public static SolrDocumentList getdocumentsByIds(SolrClient solr, Collection<String> hrids) throws SolrServerException, IOException {
    return solr.getById(hrids);
  }

  public static SolrDocumentList queryByQuery(SolrClient solr, String query) throws SolrServerException, IOException {
    SolrQuery q = new SolrQuery();
    q.setFields("*");
    q.setQuery(query);
    QueryResponse resp = solr.query(q);
    return resp.getResults();
  }

}
