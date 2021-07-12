package edu.cornell.library.integration.folio;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.cornell.library.integration.folio.Locations.Location;


public class ItemReference {

  @JsonProperty("id") public final String id;
  @JsonProperty("boundWith") public final Boolean boundWith;
  @JsonProperty("enum") public final String itemEnum;
  @JsonProperty("status") public final ItemStatus status;
  @JsonProperty("location") public final Location location;

  public ItemReference(
      @JsonProperty("id") String id,
      @JsonProperty("boundWith") Boolean boundWith,
      @JsonProperty("enum") String itemEnum,
      @JsonProperty("status") ItemStatus status,
      @JsonProperty("location") Location location) {
    this.id = id;
    this.boundWith = boundWith;
    this.itemEnum = itemEnum;
    this.status = status;
    this.location = location;
  }

}
