package net.osmand.server.api.operation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

/**
 * State for notify-inactive-users: who was already mailed, so a later run can delete stale users.
 */
@Repository
public class InactiveUserNoticeRepository {

	public static final String STATUS_NOTIFIED = "NOTIFIED";
	public static final String STATUS_DELETED = "DELETED";

	public static final String CATEGORY_HAD_PRO = "had-pro";
	public static final String CATEGORY_NEVER_PRO = "never-pro";

	public record Notice(int userid, String email, String category, String status,
	                     LocalDateTime notifiedTime, LocalDateTime deletedTime) {
	}

	private final JdbcTemplate jdbc;

	public InactiveUserNoticeRepository(@Qualifier("operationJdbcTemplate") JdbcTemplate jdbc) {
		this.jdbc = jdbc;
	}

	public Optional<Notice> find(int userId) {
		return jdbc.query("SELECT * FROM inactive_user_notice WHERE userid = ?", MAPPER, userId).stream().findFirst();
	}

	public void insertNotified(int userId, String email, String category) {
		jdbc.update("INSERT INTO inactive_user_notice(userid, email, category, status, notified_time, updated_time) " +
						"VALUES (?, ?, ?, '" + STATUS_NOTIFIED + "', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) " +
						"ON CONFLICT(userid) DO UPDATE SET email = excluded.email, category = excluded.category, " +
						"status = '" + STATUS_NOTIFIED + "', notified_time = CURRENT_TIMESTAMP, deleted_time = NULL, " +
						"updated_time = CURRENT_TIMESTAMP",
				userId, email, category);
	}

	public void markDeleted(int userId) {
		jdbc.update("UPDATE inactive_user_notice SET status = '" + STATUS_DELETED + "', " +
				"deleted_time = CURRENT_TIMESTAMP, updated_time = CURRENT_TIMESTAMP WHERE userid = ?", userId);
	}

	public void delete(int userId) {
		jdbc.update("DELETE FROM inactive_user_notice WHERE userid = ?", userId);
	}

	private static final RowMapper<Notice> MAPPER = (rs, n) -> new Notice(
			rs.getInt("userid"), rs.getString("email"), rs.getString("category"), rs.getString("status"),
			ts(rs, "notified_time"), ts(rs, "deleted_time"));

	private static LocalDateTime ts(ResultSet rs, String column) throws SQLException {
		Timestamp ts = rs.getTimestamp(column);
		return ts == null ? null : ts.toLocalDateTime();
	}
}
