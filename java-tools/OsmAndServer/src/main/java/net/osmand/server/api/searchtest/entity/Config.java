package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@MappedSuperclass
public abstract class Config {
	@Column(name = "select_fun")
	public String selectFun; // Selected JS function name to calculate output
	@Column(name = "where_fun")
	public String whereFun; // Selected JS function name to calculate output

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name="all_cols", columnDefinition = "TEXT")
	public String allCols; // column names as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name="sel_cols", columnDefinition = "TEXT")
	public String selCols; // selected column names as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name="select_params", columnDefinition = "TEXT")
	public String selectParams; // function param values as JSON string array

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name="where_params", columnDefinition = "TEXT")
	public String whereParams; // function param values as JSON string array

	@Column(columnDefinition = "TEXT")
	protected String error;

}
