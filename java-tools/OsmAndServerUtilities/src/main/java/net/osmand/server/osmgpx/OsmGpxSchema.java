package net.osmand.server.osmgpx;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OsmGpxSchema {

	private static final Log LOG = LogFactory.getLog(OsmGpxSchema.class);
	public static final String GPX_METADATA_TABLE_NAME = "osm_gpx_data";
	public static final String GPX_FILES_TABLE_NAME = "osm_gpx_files";
	public static final String GPX_REVIEWS_TABLE_NAME = "osm_gpx_reviews";
	static final int TRACK_STATS_VERSION = 1;
	static final String OUTDATED_STATS_CONDITION =
			"track_stats IS NULL OR COALESCE((track_stats->>'v')::int, 0) < " + TRACK_STATS_VERSION;
	private static final int SRID_WGS84 = 4326;

	public static void initializeSchema(Connection connection) throws SQLException {
		if (tableExists(connection, GPX_METADATA_TABLE_NAME)
				&& tableExists(connection, GPX_FILES_TABLE_NAME)
				&& tableExists(connection, GPX_REVIEWS_TABLE_NAME)) {
			return;
		}
		try (Statement statement = connection.createStatement()) {
			statement.executeUpdate("CREATE EXTENSION IF NOT EXISTS postgis");
			statement.executeUpdate("""
					CREATE TABLE IF NOT EXISTS %s (
						id bigint PRIMARY KEY,
						data bytea
					)
					""".formatted(GPX_FILES_TABLE_NAME));
			statement.executeUpdate("""
					CREATE TABLE IF NOT EXISTS %s (
						id bigint PRIMARY KEY,
						"user" text,
						"date" timestamp,
						name text,
						lat float,
						lon float,
						minlat float,
						minlon float,
						maxlat float,
						maxlon float,
						pending boolean,
						tags text[],
						description text,
						visibility text,
						activity text,
						speed float,
						distance float,
						points integer,
						max_speed float,
						max_dist_between_points float,
						time_minutes integer,
						waypoints integer,
						simplified_geometry bytea,
						speed_matches_activity boolean,
						file_activity text,
						activity_source text,
						track_stats json,
						bbox geometry GENERATED ALWAYS AS
							(ST_MakeEnvelope(minlon, minlat, maxlon, maxlat, %d)) STORED
					)
					""".formatted(GPX_METADATA_TABLE_NAME, SRID_WGS84));
			statement.executeUpdate("""
					CREATE TABLE IF NOT EXISTS %s (
						id bigint PRIMARY KEY,
						manual_review jsonb
					)
					""".formatted(GPX_REVIEWS_TABLE_NAME));

			statement.executeUpdate("""
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_parse_v%2$d ON %1$s (id) WHERE %3$s;
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_speed ON %1$s (speed);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_distance ON %1$s (distance);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_points ON %1$s (points);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_max_speed ON %1$s (max_speed);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_max_dist_between_points
						ON %1$s (max_dist_between_points);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_time_minutes ON %1$s (time_minutes);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_waypoints ON %1$s (waypoints);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_activity ON %1$s (activity);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_tags_gin ON %1$s USING GIN (tags);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_bbox_geom ON %1$s USING GIST (bbox);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_date ON %1$s (date);
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_ranges_optimized
						ON %1$s (minlat, maxlat, minlon, maxlon, activity) INCLUDE (distance, speed)
						WHERE (distance > 0::double precision OR speed > 0::double precision)
							AND activity NOT LIKE 'garbage%%' AND activity <> 'error'::text;
					CREATE INDEX IF NOT EXISTS idx_osm_gpx_ranges_composite
						ON %1$s (activity, minlat, minlon, maxlat, maxlon) INCLUDE (distance, speed)
						WHERE (distance > 0::double precision OR speed > 0::double precision)
							AND activity NOT LIKE 'garbage%%' AND activity <> 'error'::text;
					""".formatted(GPX_METADATA_TABLE_NAME, TRACK_STATS_VERSION, OUTDATED_STATS_CONDITION));
		}
		actualizeSchema(connection);
		LOG.info("OsmGpx schema initialized.");
	}

	public static void actualizeSchema(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.executeUpdate("ALTER TABLE " + GPX_METADATA_TABLE_NAME + " DROP COLUMN IF EXISTS manual_review");
			statement.executeUpdate("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_osm_gpx_bbox_counts ON " + GPX_METADATA_TABLE_NAME
					+ " USING GIST (bbox) INCLUDE (bbox, activity, date) WHERE activity IS NOT NULL AND activity <> ''");
		}
	}

	private static boolean tableExists(Connection connection, String table) throws SQLException {
		try (ResultSet tables = connection.getMetaData().getTables(null, null, table, null)) {
			return tables.next();
		}
	}
}
