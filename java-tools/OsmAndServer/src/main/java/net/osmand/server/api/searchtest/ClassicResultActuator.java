package net.osmand.server.api.searchtest;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.QuadRect;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.search.core.SearchResult;
import net.osmand.util.MapUtils;

import static net.osmand.util.MapUtils.*;

public class ClassicResultActuator extends SpatialResultActuator implements Consumer<List<SearchResult>> {
	
	public ClassicResultActuator(LatLon targetPoint, Map<String, Object> statMetrics, long osmId) {
		super(targetPoint, statMetrics, osmId);
	}
	
	private List<Amenity> getPoiObjects(BinaryMapIndexReader file, LatLon targetPoint, List<Amenity> poi) throws IOException {
		QuadRect quad = calculateLatLonBbox(targetPoint.getLatitude(), targetPoint.getLongitude(), DIST_PRECISE_THRESHOLD_M);
		BinaryMapIndexReader.SearchRequest<Amenity> request = BinaryMapIndexReader.buildSearchPoiRequest(
				get31TileNumberX(quad.left), get31TileNumberX(quad.right),
				get31TileNumberY(quad.top), get31TileNumberY(quad.bottom), 16, null,
				new ResultMatcher<>() {

					@Override
					public boolean publish(Amenity obj) {
						return true;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		List<Amenity> res = file.searchPoi(request);
		if (poi != null) {
			res.addAll(poi);
		}
		res.sort(Comparator.comparingDouble(o ->  MapUtils.getDistance(targetPoint, o.getLocation())));
		return res;
	}

	private List<BinaryMapDataObject> getMapObjects(BinaryMapIndexReader file, LatLon targetPoint, List<BinaryMapDataObject> list) throws IOException {
		QuadRect quad = calculateLatLonBbox(targetPoint.getLatitude(), targetPoint.getLongitude(), DIST_PRECISE_THRESHOLD_M);
		BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> request = BinaryMapIndexReader.buildSearchRequest(
				get31TileNumberX(quad.left), get31TileNumberX(quad.right),
				get31TileNumberY(quad.top), get31TileNumberY(quad.bottom), 16, null,
				new ResultMatcher<>() {

					@Override
					public boolean publish(BinaryMapDataObject obj) {
						return true;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		List<BinaryMapDataObject> res = file.searchMapIndex(request);
		if (list != null) {
			res.addAll(list);
		}
		sortPoints(targetPoint, res);
		return res;
	}

	private void sortPoints(LatLon targetPoint, List<BinaryMapDataObject> res) {
		res.sort(Comparator.comparingDouble(o ->  MapUtils.getDistance(targetPoint, o.getLabelLatLon())));
	}

	protected Result findActualResult(List<SearchResult> searchResults) throws IOException {
		Result actualResult = null;
		int resPlace;
		Set<BinaryMapIndexReader> files = new HashSet<>();
		for (SearchResult sr : searchResults) {
			if (sr.file != null) {
				files.add(sr.file);
			}
		}
		if (files.isEmpty()) {
			return null;
		}
		// Retrieve target map binary object - unnecessary step if store all tags earlier
		List<BinaryMapDataObject> objects = null;
		List<Amenity> poi = null;
		for(BinaryMapIndexReader file : files) {
			objects = getMapObjects(file, targetPoint, objects);
			poi = getPoiObjects(file, targetPoint, poi);
		}

		BinaryMapDataObject srcObj = null;
		Amenity srcAmenity = null;
		String srcAmenityHno = null, srcObjHno = null;
		for (BinaryMapDataObject o : objects) {
			if (ObfConstants.getOsmObjectId(o) == osmId) {
				srcObj = o;
				srcObjHno = srcObj.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue());
				metrics.put("src_map_found", srcObjHno);
				break;
			}
		}
		for (Amenity o : poi) {
			if (ObfConstants.getOsmObjectId(o) == osmId) {
				srcAmenity = o;
				srcAmenityHno = srcAmenity.getAdditionalInfo(Amenity.ADDR_HOUSENUMBER);
				metrics.put("src_poi_found", srcAmenityHno);
				break;
			}
		}
		
		// Find closest by distance by id & by tags 
		resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.object instanceof MapObject mo && ObfConstants.getOsmObjectId(mo) == osmId) {
				actualResult = new Result(ResultType.ById, resPlace, sr);
				break;
			} else if (sr.object instanceof BinaryMapDataObject bo && ObfConstants.getOsmObjectId(bo) == osmId) {
				actualResult = new Result(ResultType.ById, resPlace, sr);
				break;
			} else if(sr.object instanceof Building b && MapUtils.getDistance(sr.location, targetPoint) < DIST_PRECISE_THRESHOLD_M) {
				// only do matching by tags for object that we know don't store id like Building
				// 1. here we can compare addr:street as well for amenity
				// 2. building name doesn't have unit probably it's a bug to fix, so we check with startsWith
				String bName = b.getName();  
				if (srcAmenityHno != null && (srcAmenityHno.equals(bName) || srcAmenityHno.startsWith(bName + " "))) {
					actualResult = new Result(ResultType.ByTag, resPlace, sr);
					break;
				} else if (srcObjHno != null && (srcObjHno.equals(bName) || srcObjHno.startsWith(bName + " "))) {
					actualResult = new Result(ResultType.ByTag, resPlace, sr);
					break;
				}
			}
			resPlace++;
		}
		return actualResult;
	}
}
