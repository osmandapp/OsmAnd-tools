package net.osmand.search.core.spatial;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.gson.Gson;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;

final class SpatialSearchResultStore implements AutoCloseable {
    private static final String DB_PATH = System.getenv("DB_PATH");
    private static final Gson GSON = new Gson();

    private final String testName;
    private Connection connection;

    SpatialSearchResultStore(String testName) {
        this.testName = testName;
    }

    void store(String phrase, List<SpatialSearchResult> results, List<String> formattedResults) throws SQLException {
        if (DB_PATH == null || DB_PATH.isBlank()) {
            return;
        }
        if (results.size() != formattedResults.size()) {
            throw new IllegalArgumentException("Each spatial result must have one formatted result");
        }
        open();
        try {
            long testId = insertTest(phrase);
            for (int i = 0; i < results.size(); i++) {
                storeResult(testId, i, results.get(i), formattedResults.get(i));
            }
            connection.commit();
        } catch (SQLException | RuntimeException e) {
            connection.rollback();
            throw e;
        }
    }

    private void open() throws SQLException {
        if (connection != null) {
            return;
        }
        File database = new File(DB_PATH);
        File parent = database.getAbsoluteFile().getParentFile();
        if (parent != null && !parent.isDirectory()) {
            throw new SQLException("DB_PATH parent directory does not exist: " + parent);
        }
        connection = DriverManager.getConnection("jdbc:sqlite:" + database.getAbsolutePath());
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA foreign_keys = ON");
            statement.executeQuery("SELECT 1 FROM test LIMIT 1").close();
        }
        connection.setAutoCommit(false);
    }

    private long insertTest(String phrase) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM test WHERE name = ? AND phrase IS ?")) {
            statement.setString(1, testName);
            setString(statement, 2, phrase);
            statement.executeUpdate();
        }
        String sql = "INSERT INTO test(name, phrase_index, phrase) "
                + "SELECT ?, COALESCE(MAX(phrase_index), -1) + 1, ? FROM test WHERE name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, testName);
            setString(statement, 2, phrase);
            statement.setString(3, testName);
            statement.executeUpdate();
        }
        return lastInsertId();
    }

    private void storeResult(long testId, int resultIndex, SpatialSearchResult result, String formatted)
            throws SQLException {
        SpatialSearchResultsList parent = result.parent;
        LatLon location = result.getLatLon();
        String category = mainCategory(result);
        int[] intersectionBbox = intersectionBbox(result);
        int eloBucket = Math.max(0, (result.getTotalRating() - parent.MIN_ELO_RATING) / 64);
        int displayNameEnd = formatted.lastIndexOf(" [[");

        String sql = "INSERT INTO result(test_id,\"index\",main_category,main_subtype,display_name,latitude,"
                + "longitude,is_precise_location,intersection_kind,intersection_tile_id,intersection_zoom,"
                + "intersection_left31,intersection_top31,intersection_right31,intersection_bottom31,matched_tokens,"
                + "surplus_words,component_count,other_words,elo_bucket,type_priority,main_rating,total_rating,"
                + "compare_key,visible_level,deduplication_id,extra_name_match,has_united_object,formatted_result) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;
            statement.setLong(i++, testId);
            statement.setInt(i++, resultIndex);
            statement.setString(i++, category);
            setString(statement, i++, mainSubtype(result, category));
            statement.setString(i++, displayNameEnd < 0 ? formatted : formatted.substring(0, displayNameEnd));
            setDouble(statement, i++, location == null ? null : location.getLatitude());
            setDouble(statement, i++, location == null ? null : location.getLongitude());
            statement.setInt(i++, result.preciseLatlon == null ? 0 : 1);
            setInteger(statement, i++, result.parentInd < parent.typeIntersections.size()
                    ? parent.typeIntersections.get(result.parentInd) : null);
            setLong(statement, i++, result.parentInd < parent.tileIds.size()
                    ? parent.tileIds.get(result.parentInd) : null);
            setInteger(statement, i++, result.parentInd < parent.tileZooms.size()
                    ? parent.tileZooms.get(result.parentInd) : null);
            for (int j = 0; j < 4; j++) {
                setInteger(statement, i++, intersectionBbox == null ? null : intersectionBbox[j]);
            }
            statement.setInt(i++, result.matchedTokens());
            statement.setInt(i++, result.surplusWords);
            statement.setInt(i++, result.objs.size());
            statement.setInt(i++, result.sumOther());
            statement.setInt(i++, eloBucket);
            statement.setInt(i++, result.sumTypeOrder());
            statement.setInt(i++, result.getMainRating());
            statement.setInt(i++, result.getTotalRating());
            statement.setLong(i++, result.compareKey());
            statement.setInt(i++, result.visibleLevel());
            statement.setLong(i++, result.getIdDeduplication());
            setString(statement, i++, result.extraNameMatch);
            statement.setInt(i++, result.unitedObject == null ? 0 : 1);
            statement.setString(i, formatted);
            statement.executeUpdate();
        }
        long resultId = lastInsertId();
        for (int position = 0; position < result.objs.size(); position++) {
            storeComponent(resultId, position, result.objs.get(position));
        }
    }

    private void storeComponent(long resultId, int position, SpatialSearchResult.SpatialSearchResultRef ref)
            throws SQLException {
        SpatialSearchToken.NameIndexAtom atom = ref.atom;
        int[] bbox = atom.coords == null ? null : atom.coords.bbox31;
        String sql = "INSERT INTO component(result_id,position,type,atom_id,parent_atom_id,atom_type,index_name,"
                + "city_as_street,building_or_ref_token,nearby_radius,atom_elo,atom_other_words,atom_other_found,"
                + "other_words_found,other_words_not_found,type_order,bbox_left31,bbox_top31,bbox_right31,bbox_bottom31,"
                + "bbox_tile_id,bbox_tile_zoom,point_x16,point_y16,same_name_area_atom_id,poi_type_ids_json) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;
            statement.setLong(i++, resultId);
            statement.setInt(i++, position);
            statement.setString(i++, componentType(atom));
            statement.setLong(i++, atom.id);
            statement.setLong(i++, atom.parentid);
            statement.setInt(i++, atom.type);
            setString(statement, i++, atom.name);
            statement.setInt(i++, atom.cityAsStreet ? 1 : 0);
            setInteger(statement, i++, atom.buildingOrRefInd < 0 ? null : atom.buildingOrRefInd);
            statement.setInt(i++, atom.nearbyRadius);
            statement.setInt(i++, atom.elo);
            statement.setInt(i++, atom.otherWordsCnt);
            statement.setInt(i++, atom.otherFoundCnt);
            statement.setInt(i++, ref.otherWordsFound);
            statement.setInt(i++, ref.otherWordsNotFound);
            statement.setInt(i++, ref.typeOrder(false));
            for (int j = 0; j < 4; j++) {
                setInteger(statement, i++, bbox == null ? null : bbox[j]);
            }
            setLong(statement, i++, atom.coords == null ? null : atom.coords.bboxTileId);
            setInteger(statement, i++, atom.coords == null ? null : atom.coords.bboxTileZoom);
            setInteger(statement, i++, atom.coords == null ? null : atom.coords.x16);
            setInteger(statement, i++, atom.coords == null ? null : atom.coords.y16);
            setLong(statement, i++, atom.sameNameAreaObj == null ? null : atom.sameNameAreaObj.id);
            setString(statement, i, atom.poiTypes == null ? null : GSON.toJson(atom.poiTypes.toArray()));
            statement.executeUpdate();
        }
        for (int i = 0; i < ref.tokens.size(); i++) {
            storeToken(resultId, position, i, ref.tokens.get(i));
        }
        storeObject(resultId, position, "ATOM_OBJECT", atom.object, atom.parentid);
        storeObject(resultId, position, "BUILDING_OBJECT", atom.bldObject, atom.id);
    }

    private void storeToken(long resultId, int componentPosition, int tokenPosition, SpatialSearchToken token)
            throws SQLException {
        String sql = "INSERT INTO token(result_id,pos,token_position,original_order,sorted_order,original_word,"
                + "normalized_word,aligned_word,incomplete,main_number) VALUES (?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, resultId);
            statement.setInt(2, componentPosition);
            statement.setInt(3, tokenPosition);
            statement.setInt(4, token.originalOrder);
            statement.setInt(5, token.sortedOrder);
            statement.setString(6, token.originalWord);
            statement.setString(7, token.word);
            setString(statement, 8, token.wordAligned);
            statement.setInt(9, token.incomplete ? 1 : 0);
            setInteger(statement, 10, token.mainNumber < 0 ? null : token.mainNumber);
            statement.executeUpdate();
        }
    }

    private void storeObject(long resultId, int componentPosition, String role, MapObject object, Long parentId)
            throws SQLException {
        if (object == null) {
            return;
        }
        LatLon location = object.getLocation();
        int[] bbox = object.getBbox31();
        Amenity amenity = object instanceof Amenity ? (Amenity) object : null;
        City city = object instanceof City ? (City) object : null;
        Street street = object instanceof Street ? (Street) object : null;
        Building building = object instanceof Building ? (Building) object : null;
        City streetCity = street == null ? null : street.getCity();
        String sql = "INSERT INTO object(result_id,pos,role,class,map_object_id,osm_id,parent_id,name,names_json,"
                + "latitude,longitude,bbox_left31,bbox_top31,bbox_right31,bbox_bottom31,poi_category,poi_subtypes,"
                + "travel_elo,wikidata_id,city_type,street_city_id,street_city_name,street_city_type,postcode,"
                + "is_interpolation) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int i = 1;
            statement.setLong(i++, resultId);
            statement.setInt(i++, componentPosition);
            statement.setString(i++, role);
            statement.setString(i++, objectClass(object));
            setLong(statement, i++, object.getId());
            setLong(statement, i++, object.getId() == null ? null : ObfConstants.getOsmObjectId(object));
            setLong(statement, i++, parentId);
            setString(statement, i++, object.getName());
            i++; //statement.setString(i++, GSON.toJson(object.getNamesMap(true)));
            setDouble(statement, i++, location == null ? null : location.getLatitude());
            setDouble(statement, i++, location == null ? null : location.getLongitude());
            for (int j = 0; j < 4; j++) {
                setInteger(statement, i++, bbox == null ? null : bbox[j]);
            }
            setString(statement, i++, amenity == null || amenity.getType() == null ? null
                    : amenity.getType().getKeyName());
            setString(statement, i++, amenity == null ? null : amenity.getSubType());
            setInteger(statement, i++, amenity == null ? null : amenity.getTravelEloNumber());
            setString(statement, i++, object.getWikidata());
            setString(statement, i++, city == null ? null : city.getType().name());
            setLong(statement, i++, streetCity == null ? null : streetCity.getId());
            setString(statement, i++, streetCity == null ? null : streetCity.getName());
            setString(statement, i++, streetCity == null ? null : streetCity.getType().name());
            setString(statement, i++, building != null ? building.getPostcode() : city == null ? null : city.getPostcode());
            setInteger(statement, i, building == null ? null : building.isInterpolation() ? 1 : 0);
            statement.executeUpdate();
        }
    }

    private long lastInsertId() throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT last_insert_rowid()")) {
            return result.getLong(1);
        }
    }

    private static String mainCategory(SpatialSearchResult result) {
        SpatialSearchToken.NameIndexAtom atom = result.getFirstRef().atom;
        if (atom.isPoiCategory()) {
            return "POI_TYPE";
        } else if (isStreetIntersection(result)) {
            return "STREET_INTERSECTION";
        } else if (atom.isBuilding()) {
            return "HOUSE";
        } else if (atom.isPOI()) {
			return "POI";
		} else if (atom.isStreet()) {
			return "STREET";
		} else if (atom.object instanceof City) {
			return "CITY";
		}
		throw new IllegalArgumentException("Unsupported spatial result atom type: " + atom.type);
	}

	private static String mainSubtype(SpatialSearchResult result, String category) {
		if ("POI_TYPE".equals(category) || "STREET_INTERSECTION".equals(category)) {
			return null;
		}
		for (MapObject object : result.getObjects()) {
			if (object instanceof Amenity amenity) {
				return amenity.getSubType();
			}
		}
		return null;
	}

	private static boolean isStreetIntersection(SpatialSearchResult result) {
		Set<Long> streetIds = new HashSet<>();
		for (int i = 0; i < result.parent.tCount; i++) {
			SpatialSearchToken.NameIndexAtom atom = result.parent.linearResults
					.get(result.parentInd * result.parent.tCount + i);
			if (atom.object instanceof Street && !atom.isCityStreetName()) {
				streetIds.add(atom.object.getId());
			}
		}
		return streetIds.size() >= 2;
	}

	private static String componentType(SpatialSearchToken.NameIndexAtom atom) {
		if (atom.isPoiCategory()) {
			return "POI_TYPE";
		} else if (atom.isBuilding()) {
			return "BUILDING";
		} else if (atom.isPOI()) {
			return "POI";
		} else if (atom.isStreet()) {
			return "STREET";
		} else if (atom.isPostcode()) {
			return "POSTCODE";
		} else if (atom.isBoundary()) {
			return "BOUNDARY";
		} else if (atom.isCity()) {
			return "CITY";
		} else if (atom.isCityVillage()) {
			return "VILLAGE";
		}
		throw new IllegalArgumentException("Unsupported spatial component atom type: " + atom.type);
	}

	private static int[] intersectionBbox(SpatialSearchResult result) {
		int[] intersection = null;
		for (SpatialSearchResult.SpatialSearchResultRef ref : result.objs) {
			int[] bbox = ref.atom.coords == null ? null : ref.atom.coords.bbox31;
			if (bbox != null) {
				if (intersection == null) {
					intersection = bbox.clone();
				} else {
					SpatialSearchResultsList.clipBbox(intersection, bbox);
				}
			}
		}
		return intersection;
	}

	private static String objectClass(MapObject object) {
		if (object instanceof Amenity) {
			return "AMENITY";
		} else if (object instanceof Building) {
			return "BUILDING";
		} else if (object instanceof Street) {
			return "STREET";
		} else if (object instanceof City) {
			return "CITY";
		}
		return "OTHER";
	}

	private static void setString(PreparedStatement statement, int index, String value) throws SQLException {
		if (value == null) {
			statement.setNull(index, java.sql.Types.VARCHAR);
		} else {
			statement.setString(index, value);
		}
	}

	private static void setInteger(PreparedStatement statement, int index, Integer value) throws SQLException {
		if (value == null) {
			statement.setNull(index, java.sql.Types.INTEGER);
		} else {
			statement.setInt(index, value);
		}
	}

	private static void setLong(PreparedStatement statement, int index, Long value) throws SQLException {
		if (value == null) {
			statement.setNull(index, java.sql.Types.BIGINT);
		} else {
			statement.setLong(index, value);
		}
	}

	private static void setDouble(PreparedStatement statement, int index, Double value) throws SQLException {
		if (value == null) {
			statement.setNull(index, java.sql.Types.DOUBLE);
		} else {
			statement.setDouble(index, value);
		}
	}

	@Override
	public void close() {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new IllegalStateException("Failed to close spatial search database", e);
			} finally {
				connection = null;
			}
		}
	}
}
