package edu.cornell.library.integration.availability;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import edu.cornell.library.integration.voyager.Holding;
import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.ItemReference;
import edu.cornell.library.integration.voyager.Items;
import edu.cornell.library.integration.voyager.Holdings.HoldingSet;
import edu.cornell.library.integration.voyager.Items.Item;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class RecordsToSolr {

  public static class Change {
    public final Type type;
    public final String detail;
    public final Timestamp changeDate;
    public final String location;

    public Change (Type type, String detail, Timestamp changeDate, String location) {
      this.type = type;
      this.detail = detail;
      this.changeDate = changeDate;
      this.location = location;
    }
    public String toString() {
      return this.type.name()+" "+this.location+" "+this.detail+" "+this.changeDate.toLocalDateTime().format(formatter);
    }
    public enum Type { BIB, HOLDING, ITEM, CIRC, OTHER };
    private static DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT);
  }

  public static void updateBibsInSolr(Connection voyager, Connection inventory, Map<Integer,Set<Change>> changedBibs)
      throws SQLException, IOException, XMLStreamException, SolrServerException {

    try (PreparedStatement pstmt = inventory.prepareStatement(
        "SELECT bib_id, solr_document, title FROM bibRecsSolr"+
        " WHERE bib_id = ?"+
        "   AND active = 1");
        SolrClient solr = new HttpSolrClient( System.getenv("SOLR_URL") )) {
      for (int bibId : changedBibs.keySet()) {
        pstmt.setInt(1, bibId);
        try (ResultSet rs = pstmt.executeQuery()) {
          while ( rs.next() ) {

              SolrInputDocument doc = xml2SolrInputDocument(  rs.getString(2) );
              HoldingSet holdings = Holdings.retrieveHoldingsByBibId(voyager,bibId);
              ItemList items = Items.retrieveItemsForHoldings(voyager,holdings);
              if ( holdings.summarizeItemAvailability(items) ) 
                doc.addField("availability_facet", "Returned");
              if ( holdings.applyOpenOrderInformation(voyager,bibId) )
                doc.addField("availability_facet", "On Order");
              if ( holdings.noItemsAvailability() )
                doc.addField("availability_facet", "No Items Print");
              if ( holdings.size() > 0 )
                doc.addField("holdings_json", holdings.toJson());
              if ( items.itemCount() > 0 )
                doc.addField("items_json", items.toJson());
              for ( TreeSet<Item> itemsForHolding : items.getItems().values() )
                for ( Item i : itemsForHolding )
                  if (i.status != null && i.status.shortLoan != null)
                    doc.addField("availability_facet", "Short Loan");
              BibliographicSummary b = BibliographicSummary.summarizeHoldingAvailability(holdings);
              doc.addField("availability_json", b.toJson());
              if ( ! b.availAt.isEmpty() && ! b.unavailAt.isEmpty() )
                doc.addField("availability_facet", "Avail and Unavail");
              Set<String> locationFacet = holdings.getLocationFacetValues();
              if (locationFacet == null)
                System.out.println("b"+bibId+" location facets are null.");
              else if (! locationFacet.isEmpty()) {
                doc.removeField("location");
                doc.addField("location", locationFacet);
              }
              for ( Integer mfhdId : holdings.getMfhdIds()) {
                Holding h = holdings.get(mfhdId);
                if (h.call != null && h.call.matches(".*In Process.*"))
                  doc.addField("availability_facet","In Process");
                if (h.itemSummary != null && h.itemSummary.unavail != null)
                  for (ItemReference ir : h.itemSummary.unavail)
                    if (ir.status != null && ir.status.code != null) {
                      String status = ir.status.code.values().iterator().next();
                      doc.addField("availability_facet",status);
                    }
              }
              for (int mfhdId : holdings.getMfhdIds()) {
                if (holdings.get(mfhdId).itemSummary != null &&
                    holdings.get(mfhdId).itemSummary.tempLocs != null &&
                    ! holdings.get(mfhdId).itemSummary.tempLocs.isEmpty())
                  doc.addField("availability_facet","Partial Temp Locs");
                if (holdings.get(mfhdId).copy != null)
                  doc.addField("availability_facet", "Copies");
              }
              Set<String> changes = new HashSet<>();
              for (Change c : changedBibs.get(bibId))  changes.add(c.toString());
              
              System.out.println(bibId+" ("+rs.getString(3)+"): "+String.join("; ", changes));
              solr.add( doc );
          }

        }
      }
    }

  }

  private static SolrInputDocument xml2SolrInputDocument(String xml) throws XMLStreamException {
    SolrInputDocument doc = new SolrInputDocument();
    XMLInputFactory input_factory = XMLInputFactory.newInstance();
    XMLStreamReader r  = 
        input_factory.createXMLStreamReader(new StringReader(xml));
    while (r.hasNext()) {
      if (r.next() == XMLStreamConstants.START_ELEMENT) {
        if (r.getLocalName().equals("doc")) {
          for (int i = 0; i < r.getAttributeCount(); i++)
            if (r.getAttributeLocalName(i).equals("boost"))
              doc.setDocumentBoost(Float.valueOf(r.getAttributeValue(i)));
        } else if (r.getLocalName().equals("field")) {
          String fieldName = null;
          Float boost = null;
          for (int i = 0; i < r.getAttributeCount(); i++)
            if (r.getAttributeLocalName(i).equals("name"))
              fieldName = r.getAttributeValue(i);
            else if (r.getAttributeLocalName(i).equals("boost"))
              boost = Float.valueOf(r.getAttributeValue(i));
          if (boost != null)
            doc.addField(fieldName, r.getElementText(), boost);
          else
            doc.addField(fieldName, r.getElementText());
        }
      }
    }
    return doc;
  }

}
