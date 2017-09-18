package edu.cornell.library.integration.voyager;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.cornell.library.integration.voyager.Locations.Location;

public class ItemReference {

  @JsonProperty("id") public final int itemId;
  @JsonProperty("boundWith") public final Boolean boundWith;
  @JsonProperty("enum") public final String itemEnum;
  @JsonProperty("status") public final ItemStatus status;
  @JsonProperty("location") public final Location location;

  public ItemReference(
      @JsonProperty("id") int itemId,
      @JsonProperty("boundWith") Boolean boundWith,
      @JsonProperty("enum") String itemEnum,
      @JsonProperty("status") ItemStatus status,
      @JsonProperty("location") Location location) {
    this.itemId = itemId;
    this.boundWith = boundWith;
    this.itemEnum = itemEnum;
    this.status = status;
    this.location = location;
  }

}
