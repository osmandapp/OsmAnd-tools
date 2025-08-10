package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@MappedSuperclass
public abstract class Config {
	@JdbcTypeCode(SqlTypes.JSON)
	@Column(name = "address_expression", columnDefinition = "TEXT")
	public String addressExpression; // JS expression to calculate address

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "TEXT")
	public String columns; // column names in JSON

	@JdbcTypeCode(SqlTypes.JSON)
	@Column(columnDefinition = "TEXT")
	public String params;

	@Column(columnDefinition = "TEXT")
	protected String error;
}
