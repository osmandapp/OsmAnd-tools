package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;

@MappedSuperclass
public class RunParam {
	@Column
	public String locale;

	@Column(name = "base_search")
	public Boolean baseSearch;

	@Column(name = "north_west")
	private String northWest;

	@Column(name = "south_east")
	private String southEast;

	@Column()
	public Double lat;

	@Column()
	public Double lon;

	public String getNorthWest() {
		return northWest;
	}

	public void setNorthWest(String val) {
		this.northWest = val != null && val.trim().isEmpty() ? null : val;
	}

	public String getSouthEast() {
		return southEast;
	}

	public void setSouthEast(String val) {
		this.southEast = val != null && val.trim().isEmpty() ? null : val;
	}
}
