package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.sql.Timestamp;
import java.util.Map;

@Entity
@Table(name = "gen_result")
public class GenResult extends Result {

}
