package edu.cornell.library.integration.voyager;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ItemUnavailability {

  @JsonProperty("id") public final int itemId;
  @JsonProperty("boundWith") public final Boolean boundWith;
  @JsonProperty("enum") public final String itemEnum;
  @JsonProperty("status") public final ItemStatus status;

  public ItemUnavailability(
      @JsonProperty("id") int itemId,
      @JsonProperty("boundWith") Boolean boundWith,
      @JsonProperty("enum") String itemEnum,
      @JsonProperty("status") ItemStatus status ) {
    this.itemId = itemId;
    this.boundWith = boundWith;
    this.itemEnum = itemEnum;
    this.status = status;
  }

}
