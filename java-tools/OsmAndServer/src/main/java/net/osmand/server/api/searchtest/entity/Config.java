package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@MappedSuperclass
public abstract class Config {
	@Column(name = "file_name")
	public String fileName = "Default"; // Uploaded JS file name

	public String function; // Selected JS function name to calculate output

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name="all_cols", columnDefinition = "TEXT")
	public String allCols; // column names as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name="sel_cols", columnDefinition = "TEXT")
	public String selCols; // selected column names as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "TEXT")
	public String params; // function param values as JSON string array

	@Column(columnDefinition = "TEXT")
	protected String error;

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
