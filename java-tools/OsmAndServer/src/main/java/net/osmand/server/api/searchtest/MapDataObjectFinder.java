package net.osmand.server.api.searchtest;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.Building;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.QuadRect;
import net.osmand.data.Street;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.search.core.ObjectType;
import net.osmand.search.core.SearchResult;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class MapDataObjectFinder {
	public enum ResultType {
		Best,
		ById,
		ByTag,
		ByDist,
		Error
	}

	public record Result(ResultType type, Object exact, int place, SearchResult searchResult) {
		
		@NotNull
		public String toIdString() {
			String idStr = "U";
			Object obj = exact == null ? searchResult.object : exact; 
			if (obj instanceof BinaryMapDataObject) {
				long osmandId = ((BinaryMapDataObject) exact).getId();
				if (ObfConstants.isIdFromRelation(osmandId)) {
					idStr = "R";
				} else if (osmandId % 2 == 0) {
					idStr = "N";
				} else {
					idStr = "W";
				}
				idStr += ObfConstants.getOsmId(osmandId / 2);
			} else if (obj instanceof Street s) {
				idStr = "S" + ObfConstants.getOsmObjectId(s);
			} else if (obj instanceof MapObject mo && mo.getId() != null) {
				EntityType et = ObfConstants.getOsmEntityType(mo);
				if (et == EntityType.NODE) {
					idStr = "N";
				} else if (et == EntityType.WAY) {
					idStr = "W";
				} else {
					idStr = "R";
				}
				idStr += ObfConstants.getOsmObjectId(mo);
			} else if (searchResult.object != null) {
				idStr += searchResult.object.getClass().getSimpleName();
			}
			return idStr;
		}
		
		public String toPlaceString() {
			return place + " - " + type();
		}
		
		public String placeName() {
			
			String name = "";
			if (searchResult != null) {
				return searchResult.toString();
//				name = searchResult.localeName;
//				if (!Algorithms.isEmpty(searchResult.localeRelatedObjectName)) {
//					name += " " + searchResult.localeRelatedObjectName;
//				}
//				if (searchResult.objectType == ObjectType.HOUSE) {
//					if (searchResult.relatedObject instanceof Street) {
//						name += " " + ((Street) searchResult.relatedObject).getCity().getName();
//					}
//				}
			}
			return name;
		}
	}

	
	public Result findFirstResult(List<SearchResult> searchResults, LatLon targetPoint, Map<String, Object> row) throws IOException {
		Result firstResult = null;
		int resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.objectType != null && ObjectType.LOCATION != sr.objectType) {
				firstResult = new Result(ResultType.Best, null, resPlace, sr);
				break;
			}
			resPlace++;
		}
		if (firstResult != null) {
			if (firstResult.searchResult().object instanceof Building b) {
				if (b.getInterpolationInterval() != 0 || b.getInterpolationType() != null) {
					row.put("interpolation", b.toString());
				}
				// try to calculate precise id for first result 
				if (firstResult.searchResult().file != null) {
					List<BinaryMapDataObject> objects = getMapObjects(firstResult.searchResult().file, targetPoint);
					for (BinaryMapDataObject o : objects) {
						String hno = o.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue());
						if (Algorithms.objectEquals(hno, b.getName())) {
							return new Result(ResultType.Best, o, firstResult.place, firstResult.searchResult);
						}
					}
					List<Amenity> poi = getPoiObjects(firstResult.searchResult().file, targetPoint);
					for (Amenity o : poi) {
						String hno = o.getAdditionalInfo(Amenity.ADDR_HOUSENUMBER);
						if (Algorithms.objectEquals(hno, b.getName())) {
							return new Result(ResultType.Best, o, firstResult.place, firstResult.searchResult);
						}
					}
				}
			}
		}
		return firstResult;
	}
	
	private List<Amenity> getPoiObjects(BinaryMapIndexReader file, LatLon targetPoint) throws IOException {
		QuadRect quad = MapUtils.calculateLatLonBbox(targetPoint.getLatitude(), targetPoint.getLongitude(), DIST_THRESHOLD_M);
		BinaryMapIndexReader.SearchRequest<Amenity> request = BinaryMapIndexReader.buildSearchPoiRequest(
				MapUtils.get31TileNumberX(quad.left), MapUtils.get31TileNumberX(quad.right),
				MapUtils.get31TileNumberY(quad.top), MapUtils.get31TileNumberY(quad.bottom), 16, null,
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
		Collections.sort(res, new Comparator<Amenity>() {

			@Override
			public int compare(Amenity o1, Amenity o2) {
				return Double.compare(MapUtils.getDistance(targetPoint, o1.getLocation()), MapUtils.getDistance(targetPoint, o2.getLocation()));
			}
		});
		return res;
	}

	private List<BinaryMapDataObject> getMapObjects(BinaryMapIndexReader file, LatLon targetPoint) throws IOException {
		QuadRect quad = MapUtils.calculateLatLonBbox(targetPoint.getLatitude(), targetPoint.getLongitude(), DIST_THRESHOLD_M);
		BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> request = BinaryMapIndexReader.buildSearchRequest(
				MapUtils.get31TileNumberX(quad.left), MapUtils.get31TileNumberX(quad.right),
				MapUtils.get31TileNumberY(quad.top), MapUtils.get31TileNumberY(quad.bottom), 16, null,
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
		sortPoints(targetPoint, res);
		return res;
	}

	private void sortPoints(LatLon targetPoint, List<BinaryMapDataObject> res) {
		Collections.sort(res, new Comparator<BinaryMapDataObject>() {

			@Override
			public int compare(BinaryMapDataObject o1, BinaryMapDataObject o2) {
				double lat1 = MapUtils.get31LatitudeY(o1.getLabelY());
				double lon1 = MapUtils.get31LongitudeX(o1.getLabelX());
				double lat2 = MapUtils.get31LatitudeY(o2.getLabelY());
				double lon2 = MapUtils.get31LongitudeX(o2.getLabelX());
				return Double.compare(MapUtils.getDistance(targetPoint, lat1, lon1),
						MapUtils.getDistance(targetPoint, lat2, lon2));
			}
		});
	}


	private static final int DIST_THRESHOLD_M = 20;

	public Result findActualResult(List<SearchResult> searchResults, LatLon targetPoint, long datasetId) throws IOException {
		Result actualResult = null, actualByDist = null;
		double closestDist = 100; // needed by deduplicate for interpolation 
		int resPlace;
		BinaryMapIndexReader file = null;
		for (SearchResult sr : searchResults) {
			if (sr.file != null) {
				file = sr.file;
				break;
			}
		}
		if (file == null) {
			return null;
		}
		// Retrieve target map binary object - unnecessary step if store all tags earlier
		
		List<BinaryMapDataObject> objects = getMapObjects(file, targetPoint);
		List<Amenity> poi = getPoiObjects(file, targetPoint);
		BinaryMapDataObject srcObj = null;
		Amenity srcAmenity = null;
		for (BinaryMapDataObject o : objects) {
			if (ObfConstants.getOsmObjectId(o) == datasetId) {
				srcObj = o;
				break;
			}
		}
		for (Amenity o : poi) {
			if (ObfConstants.getOsmObjectId(o) == datasetId) {
				srcAmenity = o;
				break;
			}
		}
		
		// Find closest by distance by id & by tags 
		resPlace = 1;
		for (SearchResult sr : searchResults) {
			if (sr.object instanceof MapObject mo && ObfConstants.getOsmObjectId(mo) == datasetId) {
				actualResult = new Result(ResultType.ById, mo, resPlace, sr);
				break;
			} else if (sr.object instanceof BinaryMapDataObject bo && ObfConstants.getOsmObjectId(bo) == datasetId) {
				actualResult = new Result(ResultType.ById, bo, resPlace, sr);
				break;
			// only do matching by tags for object that we know don't store id like Building
			} else if (srcObj != null && sr.object instanceof Building b) {
				if (Algorithms.objectEquals(srcObj.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue()), b.getName())) {
					actualResult = new Result(ResultType.ByTag, srcObj, resPlace, sr);
					break;
				}
			} else if (srcAmenity != null && sr.object instanceof Building b) {
				if (Algorithms.objectEquals(srcAmenity.getAdditionalInfo(Amenity.ADDR_HOUSENUMBER), b.getName())) {
					actualResult = new Result(ResultType.ByTag, srcAmenity, resPlace, sr);
					break;
				}
			}
			if (MapUtils.getDistance(sr.location, targetPoint) < closestDist && sr.objectType != ObjectType.STREET) {
				// ignore streets cause we they don't have precise single point
				sortPoints(sr.location, objects);
				actualByDist = new Result(ResultType.ByDist, objects.size() > 0 ? objects.get(0) : null, resPlace, sr);
				closestDist = MapUtils.getDistance(sr.location, targetPoint);
			}
			resPlace++;
		}
		
		if (actualResult == null) {
			actualResult = actualByDist;
		}
		return actualResult;
	}
}
