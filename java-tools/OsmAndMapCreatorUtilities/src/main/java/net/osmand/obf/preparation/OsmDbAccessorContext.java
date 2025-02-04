package net.osmand.obf.preparation;

import java.sql.SQLException;
import java.util.Map;

import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;

public interface OsmDbAccessorContext {

	/**
	 * Load way with points (with tags) and tags
	 */
	public void loadEntityWay(Way e) throws SQLException;

	/**
	 * Load one level relation members :
	 * ways - loaded with tags and points, nodes with tags, relations with members and tags
	 */
	public void loadEntityRelation(Relation e) throws SQLException;

    public Map<Long, Node> retrieveAllRelationNodes(Relation e) throws SQLException;
}
