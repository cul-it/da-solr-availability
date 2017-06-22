package edu.cornell.library.integration.voyager;

import java.sql.Connection;

public class Items {

  public static void detectChangedItems( Connection voyager ) {

  }

  public static void detectChangedItemStatuses( Connection voyager ) {
    // SELECT * FROM ITEM_STATUS WHERE ITEM_STATUS_DATE > ?
  }

  public static void retrieveItemsByHoldingId( Connection voyager, String mfhd_id ) {

  }

  public static void retrieveItemsByItemId( Connection voyager, String item_id ) {

  }


}
