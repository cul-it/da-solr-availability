package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.EnumSet;

import org.junit.Test;

import edu.cornell.library.integration.availability.MultivolumeAnalysis.MultiVolFlag;
import edu.cornell.library.integration.voyager.Holdings;
import edu.cornell.library.integration.voyager.Items.ItemList;

public class MultivolumeAnalysisTest {

  @Test
  public void mainItemMultivol() throws IOException {
    // based on bib 2333255
    ItemList items = ItemList.extractFromJson(
        "{\"2800605\":[{\"id\":4198287,\"copy\":1,\"sequence\":1,"+
                "\"location\":{\"code\":\"mann\",\"number\":69,\"name\":\"Mann Library\","+
                            "\"library\":\"Mann Library\",\"hoursCode\":\"mann\"},"+
                "\"circGrp\":{\"16\":\"Mann Circ Group\"},"+
                "\"type\":{\"id\":3,\"name\":\"book\"},"+
                "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"},\"date\":1387881834},"+
                "\"date\":959745600},"+
            "{\"id\":6667152,\"copy\":1,\"sequence\":2,"+
                 "\"enum\":\"Guide\","+ // <-- Only 2nd item is enumerated
                 "\"location\":{\"code\":\"mann\",\"number\":69,\"name\":\"Mann Library\","+
                           "\"library\":\"Mann Library\",\"hoursCode\":\"mann\"},"+
                 "\"circGrp\":{\"16\":\"Mann Circ Group\"},"+
                 "\"type\":{\"id\":3,\"name\":\"book\"},"+
                 "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"},\"date\":1367487739},"+
                 "\"date\":1048517530}]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "xxiv, 515 p. : ill. ; 30 cm. + instructor's guide (60 p. ;  28 cm.)",
        true,
        Holdings.extractHoldingsFromJson(
            "{\"2800605\":{\"holdings\":[\"1 v.\"],"+
                          "\"supplements\":[\"1 guide\"],"+
                          "\"location\":{\"code\":\"mann\",\"number\":69,\"name\":\"Mann Library\","+
                                        "\"library\":\"Mann Library\",\"hoursCode\":\"mann\"},"+
                          "\"call\":\"TS1445 .P87x 1994\",\"circ\":true,\"date\":1048263899,"+
                          "\"items\":{\"count\":2,\"avail\":2}}}"),
        items);

    assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM, MultiVolFlag.MAINITEM, MultiVolFlag.NONBLANKENUM, MultiVolFlag.MULTIVOL), flags);
    assertEquals("1 v.",items.getItem(2800605,4198287).enumeration );  // <- 1st item now enumerated
    assertEquals("Guide",items.getItem(2800605,6667152).enumeration ); // <- 2nd item enumeration not changed
  }

  @Test
  public void mainItemMultiVolNo300e() throws IOException {
    // based on bib 787178
    ItemList items = ItemList.extractFromJson(
        "{\"955342\":["+
           "{\"id\":1989323,\"copy\":1,\"sequence\":1,"+
               "\"location\":{\"code\":\"law\",\"number\":63,\"name\":\"Law Library (Myron Taylor Hall)\","+
                           "\"library\":\"Law Library\",\"hoursCode\":\"law\"},"+
               "\"circGrp\":{\"14\":\"Law Circ Group\"},"+
               "\"type\":{\"id\":3,\"name\":\"book\"},"+
               "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"}},"+
               "\"date\":959745600},"+
            "{\"id\":1989324,\"copy\":1,\"sequence\":2,"+
               "\"enum\":\"cum. suppl.:no. 2\",\"chron\":\"1984\","+  // <- 2nd item enumerated
               "\"location\":{\"code\":\"law\",\"number\":63,\"name\":\"Law Library (Myron Taylor Hall)\","+
                           "\"library\":\"Law Library\",\"hoursCode\":\"law\"},"+
               "\"circGrp\":{\"14\":\"Law Circ Group\"},"+
               "\"type\":{\"id\":3,\"name\":\"book\"},"+
               "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"}},"+
               "\"date\":959745600}]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "1 v. (various pagings) ; 24 cm.",
        false, // <- No 300$e to indicate supplementary holdings
        Holdings.extractHoldingsFromJson(
            "{\"955342\":{"+
                 "\"holdings\":[\"1 v.;\"],"+
                 "\"supplements\":[\"\\\"1984 cumulative supplement no. 2\\\"\"],"+ // <- Supplementary holdings in mfhd
                 "\"location\":{\"code\":\"law\",\"number\":63,\"name\":\"Law Library (Myron Taylor Hall)\","+
                               "\"library\":\"Law Library\",\"hoursCode\":\"law\"},"+
                 "\"call\":\"KF6491 .E91\","+
                 "\"circ\":true,"+
                 "\"date\":959745600,"+
                 "\"items\":{\"count\":2,\"avail\":2}}}"),
        items);

    assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM, MultiVolFlag.MAINITEM, MultiVolFlag.NONBLANKENUM, MultiVolFlag.MULTIVOL), flags);
    assertEquals("1 v.;",items.getItem(955342,1989323).enumeration );  // <- 1st item now enumerated
    assertEquals("cum. suppl.:no. 2",items.getItem(955342,1989324).enumeration ); // <- 2nd item enumeration not changed
  }

  @Test
  public void twoCopiesOneLocation() throws IOException {
    // based on bib 10409210
    ItemList items = ItemList.extractFromJson(
        "{\"10711219\":["+
            "{\"id\":10521240,"+
                "\"copy\":1,\"sequence\":1,"+
                "\"location\":{\"code\":\"hote\",\"number\":43,\"name\":\"ILR Library (Ives Hall)\","+
                              "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
                "\"circGrp\":{\"13\":\"ILR Circ Group\"},"+
                "\"type\":{\"id\":3,\"name\":\"book\"},"+
                "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"},\"date\":1531409341},"+
                "\"date\":1531409341}],"+
         "\"10711221\":["+
            "{\"id\":10521243,\"copy\":2,\"sequence\":1,"+
                "\"location\":{\"code\":\"hote\",\"number\":43,\"name\":\"ILR Library (Ives Hall)\","+
                              "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
                "\"circGrp\":{\"13\":\"ILR Circ Group\"},"+
                "\"type\":{\"id\":3,\"name\":\"book\"},"+
                "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"},\"date\":1531409402},"+
                "\"date\":1531409402}]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "xx, 265 pages ; 23 cm",
        false,
        Holdings.extractHoldingsFromJson(
            "{\"10711219\":{\"location\":{\"code\":\"hote\",\"number\":43,\"name\":\"ILR Library (Ives Hall)\","+
                                         "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
                   "\"call\":\"HD57.7 .N495 2019\","+
                   "\"circ\":true,\"date\":1531409300,"+
                   "\"items\":{\"count\":1,\"avail\":1}},"+
             "\"10711221\":{\"copy\":2,\"location\":{\"code\":\"hote\",\"number\":43,\"name\":\"ILR Library (Ives Hall)\","+
                                               "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
                   "\"call\":\"HD57.7 .N495 2019\","+
                   "\"circ\":true,\"date\":1531409390,"+
                   "\"items\":{\"count\":1,\"avail\":1}}}"),
        items);

      assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM),flags);
      assertNull( items.getItem(10711219,10521240).enumeration );
      assertNull( items.getItem(10711221,10521243).enumeration );
  }

  @Test
  public void unenumeratedNoHoldingsDescription() throws IOException {
    // based on bib 7754011
    ItemList items = ItemList.extractFromJson(
        "{\"8170026\":["+
            "{\"id\":9143573,\"copy\":1,\"sequence\":1,"+
                "\"enum\":\"v.1\","+
                "\"location\":{\"code\":\"mus\",\"number\":88,\"name\":\"Cox Library of Music (Lincoln Hall)\","+
                              "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                "\"circGrp\":{\"18\":\"Music Circ Group\"},"+
                "\"type\":{\"id\":3,\"name\":\"book\"},"+
                "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"},\"date\":1345714649},"+
                "\"date\":1344287056},"+
            "{\"id\":9143576,\"copy\":1,\"sequence\":2,"+
                "\"enum\":\"v.2\","+
                "\"location\":{\"code\":\"mus\",\"number\":88,\"name\":\"Cox Library of Music (Lincoln Hall)\","+
                              "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                "\"circGrp\":{\"18\":\"Music Circ Group\"},"+
                "\"type\":{\"id\":3,\"name\":\"book\"},"+
                "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"},\"date\":1345714649},"+
                "\"date\":1344287585}],"+
        "\"8182017\":["+
            "{\"id\":9145695,\"copy\":1,\"sequence\":1,"+
                "\"location\":{\"code\":\"mus,av\",\"number\":90,\"name\":\"Music Library A/V (Non-Circulating)\","+
                              "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                "\"circGrp\":{\"18\":\"Music Circ Group\"},"+
                "\"type\":{\"id\":9,\"name\":\"nocirc\"},"+
                "\"status\":{\"available\":true,\"code\":{\"1\":\"Not Charged\"},\"date\":1344937146},"+
                "\"date\":1373035358}]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "v. : port. ; 24 cm. + 1 videodisc (43 min. : sd., b&w ; 4 3/4 in.)",
        true,
        Holdings.extractHoldingsFromJson(
            "{\"8170026\":{\"notes\":[\"Accompanying disc stored in A/V Collection.\"],"+
                          "\"holdings\":[\"v.1-2\"],"+
                          "\"location\":{\"code\":\"mus\",\"number\":88,\"name\":\"Cox Library of Music (Lincoln Hall)\","+
                                        "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                          "\"call\":\"GV1785.S534 S534 2006\","+
                          "\"circ\":true,"+
                          "\"date\":1344365837,"+
                          "\"items\":{\"count\":2,\"avail\":2}},"+
             "\"8182017\":{\"notes\":[\"Accompanying text shelved in stacks.\"],"+
                          "\"location\":{\"code\":\"mus,av\",\"number\":90,\"name\":\"Music Library A/V (Non-Circulating)\","+
                                        "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                          "\"call\":\"DVD 1086\","+
                          "\"circ\":false,"+
                          "\"date\":1344366297,"+
                          "\"items\":{\"count\":1,\"avail\":1}}}"),
        items);

    assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM, MultiVolFlag.MISSINGHOLDINGSDESC,
        MultiVolFlag.NONBLANKENUM, MultiVolFlag.MULTIVOL), flags);
    assertEquals("v.1",items.getItem(8170026,9143573).enumeration );  // <- 1st item enumeration not changed
    assertEquals("v.2",items.getItem(8170026,9143576).enumeration );  // <- 1st item enumeration not changed
    assertEquals("DVD 1086",items.getItem(8182017,9145695).enumeration ); // <- 3rd item now enumerated w/ call no

  }

  @Test
  public void onlineBook() throws IOException {
    // based on bib 9849812
    ItemList items = ItemList.extractFromJson("{\"10169681\":[]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "1 online resource.",
        false,
        Holdings.extractHoldingsFromJson("{\"10169681\":{\"online\":true,\"date\":1489763572}}"),
        items);

    assertEquals(EnumSet.noneOf(MultiVolFlag.class), flags);
    assertEquals("{\"10169681\":[]}",items.toJson());
  }
}
