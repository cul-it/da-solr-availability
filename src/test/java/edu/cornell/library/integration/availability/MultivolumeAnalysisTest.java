package edu.cornell.library.integration.availability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.EnumSet;

import org.junit.Test;

import edu.cornell.library.integration.availability.MultivolumeAnalysis.MultiVolFlag;
import edu.cornell.library.integration.folio.Holdings;
import edu.cornell.library.integration.folio.Items.ItemList;

public class MultivolumeAnalysisTest {

  @Test
  public void mainItemMultivol() throws IOException {
    // based on bib 2333255
    ItemList items = ItemList.extractFromJson(
        "{\"dd5dd831-9d3b-4871-aba9-597b88384853\":"+
           "[{\"id\":\"16be1798-0dc8-4397-a4bf-6323c5049ebd\","+
             "\"hrid\":4198287,\"copy\":1,\"sequence\":1,"+
             "\"location\":{\"code\":\"mann\",\"name\":\"Mann Library\","+
                           "\"library\":\"Mann Library\",\"hoursCode\":\"mann\"},"+
             "\"status\":{\"status\":\"available\"},"+
             "\"date\":959745600},"+
            "{\"id\":\"f2ff1732-5adf-48f3-aa70-e0020e7063a2\","+
             "\"hrid\":6667152,\"copy\":1,\"sequence\":2,"+
             "\"enum\":\"Guide\","+ // <-- Only 2nd item is enumerated
             "\"location\":{\"code\":\"mann\",\"name\":\"Mann Library\","+
                           "\"library\":\"Mann Library\",\"hoursCode\":\"mann\"},"+
             "\"status\":{\"status\":\"available\"},"+
             "\"date\":1048517530}]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "xxiv, 515 p. : ill. ; 30 cm. + instructor's guide (60 p. ;  28 cm.)",
        true,
        Holdings.extractHoldingsFromJson(
            "{\"dd5dd831-9d3b-4871-aba9-597b88384853\":"+
                         "{\"hrid\":\"2800605\","+
                          "\"holdings\":[\"1 v.\"],"+
                          "\"supplements\":[\"1 guide\"],"+
                          "\"location\":{\"code\":\"mann\",\"name\":\"Mann Library\","+
                                        "\"library\":\"Mann Library\",\"hoursCode\":\"mann\"},"+
                          "\"call\":\"TS1445 .P87x 1994\",\"circ\":true,\"date\":1048263899,"+
                          "\"items\":{\"count\":2,\"avail\":2}}}"),
        items);

    assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM, MultiVolFlag.MAINITEM,
                            MultiVolFlag.NONBLANKENUM, MultiVolFlag.MULTIVOL), flags);
    assertEquals("1 v.",  // <- 1st item now enumerated
        items.getItem("dd5dd831-9d3b-4871-aba9-597b88384853","16be1798-0dc8-4397-a4bf-6323c5049ebd").enumeration );
    assertEquals("Guide", // <- 2nd item enumeration not changed
        items.getItem("dd5dd831-9d3b-4871-aba9-597b88384853","f2ff1732-5adf-48f3-aa70-e0020e7063a2").enumeration );
  }

  @Test
  public void mainItemMultiVolNo300e() throws IOException {
    // based on bib 787178
    ItemList items = ItemList.extractFromJson(
        "{\"30cf17e8-4603-4371-97b1-c9fe117154a6\":["+
           "{\"id\":\"f650e05b-7cf4-4499-8901-b4b4017be2b0\","+
               "\"hrid\":\"1989323\",\"copy\":1,\"sequence\":1,"+
               "\"location\":{\"code\":\"law\",\"name\":\"Law Library (Myron Taylor Hall)\","+
                           "\"library\":\"Law Library\",\"hoursCode\":\"law\"},"+
               "\"status\":{\"status\":\"available\"},"+
               "\"date\":959745600},"+
            "{\"id\":\"6f40d63b-0f17-427f-9fc4-184f69d7645d\","+
               "\"hrid\":\"1989324\",\"copy\":1,\"sequence\":2,"+
               "\"enum\":\"cum. suppl.:no. 2\",\"chron\":\"1984\","+  // <- 2nd item enumerated
               "\"location\":{\"code\":\"law\",\"name\":\"Law Library (Myron Taylor Hall)\","+
                           "\"library\":\"Law Library\",\"hoursCode\":\"law\"},"+
               "\"status\":{\"status\":\"available\"},"+
               "\"date\":959745600}]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "1 v. (various pagings) ; 24 cm.",
        false, // <- No 300$e to indicate supplementary holdings
        Holdings.extractHoldingsFromJson(
            "{\"30cf17e8-4603-4371-97b1-c9fe117154a6\":"+
                "{\"hrid\":\"955342\","+
                 "\"holdings\":[\"1 v.;\"],"+
                 "\"supplements\":[\"\\\"1984 cumulative supplement no. 2\\\"\"],"+ // <- Supplementary holdings in mfhd
                 "\"location\":{\"code\":\"law\",\"name\":\"Law Library (Myron Taylor Hall)\","+
                               "\"library\":\"Law Library\",\"hoursCode\":\"law\"},"+
                 "\"call\":\"KF6491 .E91\","+
                 "\"circ\":true,"+
                 "\"date\":959745600,"+
                 "\"items\":{\"count\":2,\"avail\":2}}}"),
        items);

    assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM, MultiVolFlag.MAINITEM,
                            MultiVolFlag.NONBLANKENUM, MultiVolFlag.MULTIVOL), flags);
    assertEquals("1 v.;",  // <- 1st item now enumerated
        items.getItem("30cf17e8-4603-4371-97b1-c9fe117154a6","f650e05b-7cf4-4499-8901-b4b4017be2b0").enumeration );
    assertEquals("cum. suppl.:no. 2", // <- 2nd item enumeration not changed
        items.getItem("30cf17e8-4603-4371-97b1-c9fe117154a6","6f40d63b-0f17-427f-9fc4-184f69d7645d").enumeration );
  }

  @Test
  public void twoCopiesOneLocation() throws IOException {
    // based on bib 10409210
    ItemList items = ItemList.extractFromJson(
        "{\"b8715bc0-1741-498e-802a-c80c6d9e8dee\":["+
            "{\"id\":\"c404f560-cd42-46f0-a42c-ef652bc2a927\","+
             "\"hrid\":\"10521240\","+
             "\"copy\":1,\"sequence\":1,"+
             "\"location\":{\"code\":\"hote\",\"name\":\"ILR Library (Ives Hall)\","+
                           "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
             "\"status\":{\"status\":\"available\"},"+
             "\"date\":1531409341}],"+
         "\"5cecee4c-edb4-484e-be6a-7ab38f1acd7e\":["+
            "{\"id\":\"a797de03-323f-485d-ad12-055b2ce27ade\","+
             "\"hrid\":\"10521243\",\"copy\":2,\"sequence\":1,"+
             "\"location\":{\"code\":\"hote\",\"name\":\"ILR Library (Ives Hall)\","+
                           "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
             "\"status\":{\"status\":\"available\"},"+
             "\"date\":1531409402}]}");

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "xx, 265 pages ; 23 cm",
        false,
        Holdings.extractHoldingsFromJson(
            "{\"b8715bc0-1741-498e-802a-c80c6d9e8dee\":"+
                  "{\"hrid\":\"10711219\","+
                   "\"location\":{\"code\":\"hote\",\"name\":\"ILR Library (Ives Hall)\","+
                                 "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
                   "\"call\":\"HD57.7 .N495 2019\","+
                   "\"circ\":true,\"date\":1531409300,"+
                   "\"items\":{\"count\":1,\"avail\":1}},"+
             "\"5cecee4c-edb4-484e-be6a-7ab38f1acd7e\":"+
                  "{\"hrid\":\"10711221\",\"copy\":2,"+
                   "\"location\":{\"code\":\"hote\",\"name\":\"ILR Library (Ives Hall)\","+
                                 "\"library\":\"ILR Library\",\"hoursCode\":\"ilr\"},"+
                   "\"call\":\"HD57.7 .N495 2019\","+
                   "\"circ\":true,\"date\":1531409390,"+
                   "\"items\":{\"count\":1,\"avail\":1}}}"),
        items);

      assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM),flags);
      assertNull(items.getItem("b8715bc0-1741-498e-802a-c80c6d9e8dee",
          "c404f560-cd42-46f0-a42c-ef652bc2a927").enumeration );
      assertNull(items.getItem("5cecee4c-edb4-484e-be6a-7ab38f1acd7e",
          "a797de03-323f-485d-ad12-055b2ce27ade").enumeration );
  }

  @Test
  public void unenumeratedNoHoldingsDescription() throws IOException {
    // based on bib 7754011
    String json = 
        "{\"c9298907-8278-468f-bda4-152aba22cd78\":["+

               "{\"id\":\"aa49a752-a18a-44af-84d9-3a282af72035\","+
                "\"hrid\":\"9143573\",\"copy\":1,\"sequence\":1,"+
                "\"enum\":\"v.1\","+
                "\"location\":{\"code\":\"mus\",\"name\":\"Cox Library of Music (Lincoln Hall)\","+
                              "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                "\"loanType\":{\"id\":\"2b94c631-fca9-4892-a730-03ee529ffe27\",\"name\":\"Circulating\"},"+
                "\"status\":{\"status\":\"Available\"}},"+

               "{\"id\":\"04e81de9-7d74-45ca-91d0-a4f9dbeaad28\","+
                "\"hrid\":\"9143576\",\"copy\":1,\"sequence\":2,"+
                "\"enum\":\"v.2\","+
                "\"location\":{\"code\":\"mus\",\"name\":\"Cox Library of Music (Lincoln Hall)\","+
                              "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                "\"loanType\":{\"id\":\"2b94c631-fca9-4892-a730-03ee529ffe27\",\"name\":\"Circulating\"},"+
                "\"status\":{\"status\":\"Available\"}}],"+

         "\"1486af00-3ad0-473c-9612-62dd379fc69a\":["+
               "{\"id\":\"6ad20caa-18e6-485f-b931-ad4aebd5ea9c\","+
                "\"hrid\":\"9145695\",\"copy\":1,\"sequence\":1,"+
                "\"location\":{\"code\":\"mus,av\",\"name\":\"Music Library A/V (Non-Circulating)\","+
                              "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                "\"loanType\":{\"id\":\"2e48e713-17f3-4c13-a9f8-23845bb210a4\",\"name\":\"Non-circulating\"},"+
                "\"status\":{\"status\":\"Available\"}}]}";
    System.out.println(json);
    ItemList items = ItemList.extractFromJson(json);

    EnumSet<MultiVolFlag> flags = MultivolumeAnalysis.analyze(
        "Book",
        "v. : port. ; 24 cm. + 1 videodisc (43 min. : sd., b&w ; 4 3/4 in.)",
        true,
        Holdings.extractHoldingsFromJson(
            "{\"c9298907-8278-468f-bda4-152aba22cd78\":"+
                         "{\"hrid\":\"8170026\","+
                          "\"notes\":[\"Accompanying disc stored in A/V Collection.\"],"+
                          "\"holdings\":[\"v.1-2\"],"+
                          "\"location\":{\"code\":\"mus\",\"name\":\"Cox Library of Music (Lincoln Hall)\","+
                                        "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                          "\"call\":\"GV1785.S534 S534 2006\","+
                          "\"circ\":true,"+
                          "\"date\":1344365837,"+
                          "\"items\":{\"count\":2,\"avail\":2}},"+
             "\"1486af00-3ad0-473c-9612-62dd379fc69a\":"+
                         "{\"hrid\":\"8182017\","+
                          "\"notes\":[\"Accompanying text shelved in stacks.\"],"+
                          "\"location\":{\"code\":\"mus,av\",\"name\":\"Music Library A/V (Non-Circulating)\","+
                                        "\"library\":\"Music Library\",\"hoursCode\":\"music\"},"+
                          "\"call\":\"DVD 1086\","+
                          "\"circ\":false,"+
                          "\"date\":1344366297,"+
                          "\"items\":{\"count\":1,\"avail\":1}}}"),
        items);

    assertEquals(EnumSet.of(MultiVolFlag.BLANKENUM, MultiVolFlag.MISSINGHOLDINGSDESC,
        MultiVolFlag.NONBLANKENUM, MultiVolFlag.MULTIVOL), flags);
    assertEquals("v.1",  // <- 1st item enumeration not changed
        items.getItem("c9298907-8278-468f-bda4-152aba22cd78","aa49a752-a18a-44af-84d9-3a282af72035").enumeration );
    assertEquals("v.2",  // <- 1st item enumeration not changed
        items.getItem("c9298907-8278-468f-bda4-152aba22cd78","04e81de9-7d74-45ca-91d0-a4f9dbeaad28").enumeration );
    assertEquals("DVD 1086", // <- 3rd item now enumerated w/ call no
        items.getItem("1486af00-3ad0-473c-9612-62dd379fc69a","6ad20caa-18e6-485f-b931-ad4aebd5ea9c").enumeration );

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
    assertEquals("{}",items.toJson());
  }
}
