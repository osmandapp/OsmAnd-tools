package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@MappedSuperclass
public abstract class Config {
	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "TEXT")
	public String script; // JS script with functions

	public String function; // Selected JS function name to calculate address

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
}
