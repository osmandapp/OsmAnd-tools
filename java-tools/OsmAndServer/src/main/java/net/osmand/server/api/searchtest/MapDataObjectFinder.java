package net.osmand.server.api.searchtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import static net.osmand.util.MapUtils.*;
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
				EntityType et = ObfConstants.getOsmEntityType((BinaryMapDataObject) obj);
				if (et == EntityType.RELATION) {
					idStr = "R";
				} else if (et == EntityType.NODE) {
					idStr = "N";
				} else {
					idStr = "W";
				}
				idStr += ObfConstants.getOsmObjectId((BinaryMapDataObject) obj);
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
				// building name doesn't have unit probably it's a bug to fix, so we check with startsWith
				if (firstResult.searchResult().file != null) {
					List<Amenity> poi = getPoiObjects(firstResult.searchResult().file, firstResult.searchResult.location, null);
					Amenity am = null;
					BinaryMapDataObject obj = null;
					for (Amenity o : poi) {
						String hno = o.getAdditionalInfo(Amenity.ADDR_HOUSENUMBER);
						if (hno != null && (hno.equals(b.getName()) || hno.startsWith(b.getName() + " "))) {
							am = o;
							break;
						}
					}
					List<BinaryMapDataObject> objects = getMapObjects(firstResult.searchResult().file, firstResult.searchResult.location, null);
					for (BinaryMapDataObject o : objects) {
						String hno = o.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue());
						if (hno != null && (hno.equals(b.getName()) || hno.startsWith(b.getName() + " "))) {
							obj = o;
							break;
						}
					}
					
					if (obj != null && am != null) {
						double dObj = getDistance(firstResult.searchResult.location, obj.getLabelLatLon());
						double dAm = getDistance(firstResult.searchResult.location, am.getLocation());
						if (dObj < dAm) {
							am = null;
						} else {
							obj = null;
						}
					}
					if (obj != null) {
						return new Result(ResultType.Best, obj, firstResult.place, firstResult.searchResult);
					} else if (am != null) {
						return new Result(ResultType.Best, am, firstResult.place, firstResult.searchResult);
					}
					
				}
			}
		}
		return firstResult;
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
		Collections.sort(res, new Comparator<Amenity>() {

			@Override
			public int compare(Amenity o1, Amenity o2) {
				return Double.compare(getDistance(targetPoint, o1.getLocation()), getDistance(targetPoint, o2.getLocation()));
			}
		});
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
		Collections.sort(res, new Comparator<BinaryMapDataObject>() {

			@Override
			public int compare(BinaryMapDataObject o1, BinaryMapDataObject o2) {
				return Double.compare(getDistance(targetPoint, o1.getLabelLatLon()),
						getDistance(targetPoint, o2.getLabelLatLon()));
			}
		});
	}


	private static final int DIST_PRECISE_THRESHOLD_M = 20;

	public Result findActualResult(List<SearchResult> searchResults, LatLon targetPoint, long datasetId, Map<String, Object> genRow) throws IOException {
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
			if (ObfConstants.getOsmObjectId(o) == datasetId) {
				srcObj = o;
				srcObjHno = srcObj.getTagValue(OSMTagKey.ADDR_HOUSE_NUMBER.getValue());
				genRow.put("src_map_found", srcObjHno);
				break;
			}
		}
		for (Amenity o : poi) {
			if (ObfConstants.getOsmObjectId(o) == datasetId) {
				srcAmenity = o;
				srcAmenityHno = srcAmenity.getAdditionalInfo(Amenity.ADDR_HOUSENUMBER);
				genRow.put("src_poi_found", srcAmenityHno);
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
			} else if(sr.object instanceof Building b && getDistance(sr.location, targetPoint) < DIST_PRECISE_THRESHOLD_M) {
				// only do matching by tags for object that we know don't store id like Building
				// 1. here we can compare addr:street as well for amenity
				// 2. building name doesn't have unit probably it's a bug to fix, so we check with startsWith
				String bName = b.getName();  
				if (srcAmenityHno != null && (srcAmenityHno.equals(bName) || srcAmenityHno.startsWith(bName + " "))) {
					actualResult = new Result(ResultType.ByTag, srcAmenity, resPlace, sr);
					break;
				} else if (srcObjHno != null && (srcObjHno.equals(bName) || srcObjHno.startsWith(bName + " "))) {
					actualResult = new Result(ResultType.ByTag, srcObj, resPlace, sr);
					break;
				}
			}
			resPlace++;
		}
		return actualResult;
	}
}
