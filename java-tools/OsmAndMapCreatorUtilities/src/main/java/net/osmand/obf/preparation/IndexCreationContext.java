package net.osmand.obf.preparation;

import net.osmand.binary.Abbreviations;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.*;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.util.Algorithms;
import net.osmand.util.translit.ChineseTranslitHelper;
import net.osmand.util.translit.JapaneseTranslitHelper;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class IndexCreationContext {
    private static final Log log = LogFactory.getLog(IndexCreationContext.class);
    private static final String JAPAN = "japan";
	private static final String CHINA = "china";
	private static final double INFLATE_REGION_BBOX_KM = 20;

    public OsmandRegions allRegions;
    public boolean basemap;

    private boolean decryptAbbreviations = false;
    private boolean translitJapaneseNames = false;
	private boolean translitChineseNames = false;
	private IndexCreator indexCreator;

	private List<QuadRect> inflatedRegionQuads = null;

	IndexCreationContext(IndexCreator indexCreator, String regionName, boolean basemap) {
		this.indexCreator = indexCreator;
		this.allRegions = prepareRegions();
		this.basemap = basemap;
		if (regionName != null) {
			this.translitJapaneseNames = regionName.toLowerCase().startsWith(JAPAN);
			this.translitChineseNames = regionName.toLowerCase().startsWith(CHINA);
			this.decryptAbbreviations = needDecryptAbbreviations(getRegionLang(allRegions, regionName));
            WorldRegion region = this.allRegions.getRegionDataByDownloadName(regionName);
            if (region != null) {
				inflatedRegionQuads = region.getAllPolygonsBounds();
				double inflate = INFLATE_REGION_BBOX_KM * 1000 / MapUtils.METERS_IN_DEGREE;
	            for (QuadRect rect : inflatedRegionQuads) {
		            MapUtils.inflateBBoxLatLon(rect, inflate, inflate);
	            }
            }
		}
	}

	IndexPoiCreator getIndexPoiCreator() {
		return indexCreator.indexPoiCreator;
	}

	IndexVectorMapCreator getIndexMapCreator() {
		return indexCreator.indexMapCreator;
	}

	IndexHeightData getIndexHeightData() {
		return indexCreator.heightData;
	}

    @Nullable
    private OsmandRegions prepareRegions() {
        OsmandRegions or = new OsmandRegions();
        try {
            or.prepareFile();
            or.cacheAllCountries();
        } catch (IOException e) {
            log.error("Error preparing regions", e);
            return null;
        }
        return or;
    }

    private static String getRegionLang(OsmandRegions osmandRegions, String regionName) {
		if (osmandRegions == null) {
			return null;
		}
        WorldRegion wr = osmandRegions.getRegionDataByDownloadName(regionName);
        if (wr != null) {
            return wr.getParams().getRegionLang();
        } else {
            return null;
        }
    }

    private static boolean needDecryptAbbreviations(String regionLang) {
        if (regionLang != null) {
            String[] langArr = regionLang.split(",");
            for (String lang : langArr) {
                if (lang.equals("en")) {
                    return true;
                }
            }
        }
        return false;
    }

	public void translitJapaneseNames(Entity e) {
		if (needTranslitName(e, e.getTags(), translitJapaneseNames, JAPAN)) {
			e.putTag(OSMTagKey.NAME_EN.getValue(),
					JapaneseTranslitHelper.getEnglishTransliteration(e.getTag(OSMTagKey.NAME.getValue())));
		}
	}

	public void translitChineseNames(Entity e) {
		if (needTranslitName(e, e.getTags(), translitChineseNames, CHINA)) {
			try {
				String pinyinNameTag = "name:zh_pinyin";
				if (e.getNameTags().containsKey(pinyinNameTag)) {
					e.putTag(OSMTagKey.NAME_EN.getValue(), e.getNameTags().get(pinyinNameTag));
				} else {
					e.putTag(OSMTagKey.NAME_EN.getValue(),
							ChineseTranslitHelper.getPinyinTransliteration(e.getTag(OSMTagKey.NAME.getValue())));
				}
			} catch (Throwable e1) {
				log.error(e1.getMessage(), e1);
			}
		}
	}

	private boolean needTranslitName(Entity e, Map<String, String> etags, boolean translitByRegionName, String region) {
		if (!Algorithms.isEmpty(etags.get(OSMTagKey.NAME_EN.getValue()))
				|| Algorithms.isEmpty(etags.get(OSMTagKey.NAME.getValue()))) {
			return false;
		}
		if (translitByRegionName) {
			return true;
		} else if (!Algorithms.isEmpty(etags.get(MapRenderingTypesEncoder.OSMAND_REGION_NAME_TAG))) {
			return etags.get(MapRenderingTypesEncoder.OSMAND_REGION_NAME_TAG).contains(region);
		}
		return false;
	}

	public String decryptAbbreviations(String name, LatLon loc, boolean addRegionTag) {
		boolean upd = false;
		if (decryptAbbreviations) {
			upd = true;
		} else if (addRegionTag && loc != null) {
			Set<String> dwNames = calcDownloadNames(null, false, allRegions,
					new QuadRect(loc.getLongitude(), loc.getLatitude(), loc.getLongitude(), loc.getLatitude()));
			for (String dwName : dwNames) {
				if (needDecryptAbbreviations(getRegionLang(allRegions, dwName))) {
					upd = true;
					break;
				}
			}
		}
		if(upd) {
			name = Abbreviations.replaceAll(name);
		}
		return name;
	}

	public Set<String> calcRegionTag(Entity entity, boolean add) {
		OsmandRegions or = allRegions;
		QuadRect qr = null;
		if (entity instanceof Relation) {
			LatLon l = ((Relation) entity).getLatLon();
			if (l != null) {
				double lat = l.getLatitude();
				double lon = l.getLongitude();
				qr = new QuadRect(lon, lat, lon, lat);
			}
		} else if (entity instanceof Way) {
			qr = ((Way) entity).getLatLonBBox();
		} else if (entity instanceof Node) {
			double lat = ((Node) entity).getLatitude();
			double lon = ((Node) entity).getLongitude();
			qr = new QuadRect(lon, lat, lon, lat);
		}
		return calcDownloadNames(entity, add, or, qr);
	}

	private Set<String> calcDownloadNames(Entity entity, boolean add, OsmandRegions or, QuadRect qr) {
		if (qr != null && or != null) {
			try {
				int lx = MapUtils.get31TileNumberX(qr.left);
				int rx = MapUtils.get31TileNumberX(qr.right);
				int by = MapUtils.get31TileNumberY(qr.bottom);
				int ty = MapUtils.get31TileNumberY(qr.top);
				List<BinaryMapDataObject> bbox = or.query(lx, rx, ty, by);
				TreeSet<String> lst = new TreeSet<String>();
				for (BinaryMapDataObject bo : bbox) {
					String dw = or.getDownloadName(bo);
					if (!Algorithms.isEmpty(dw) && or.isDownloadOfType(bo, OsmandRegions.MAP_TYPE)) {
						lst.add(dw);
					}
				}
				if (add && entity != null) {
					entity.putTag(MapRenderingTypesEncoder.OSMAND_REGION_NAME_TAG, serialize(lst));
				}
				return lst;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return Collections.emptySet();
	}

	private static String serialize(TreeSet<String> lst) {
		StringBuilder bld = new StringBuilder();
		Iterator<String> it = lst.iterator();
		while(it.hasNext()) {
			String next = it.next();
			if(bld.length() > 0) {
				bld.append(",");
			}
			bld.append(next);
		}
		return bld.toString();
	}

	public boolean isInsideRegionBBox(Entity entity) {
		if (inflatedRegionQuads == null) {
			return true; // region might have no bbox
		} else if (entity instanceof Node node) {
			double lon = node.getLongitude();
			double lat = node.getLatitude();
			for (QuadRect quad : inflatedRegionQuads) {
				if (quad.contains(lon, lat, lon, lat)) {
					return true;
				}
			}
			return false; // Node is outside
		} else if (entity instanceof Way way && way.getFirstNode() != null) {
			List<LatLon> latLons = new ArrayList<>();
			latLons.add(way.getFirstNode().getLatLon());
			latLons.add(way.getLastNode().getLatLon());
			if (way.getNodes().size() > 2) {
				latLons.add(way.getNodes().get(way.getNodes().size() / 2).getLatLon());
			}
			for (LatLon l : latLons) {
				double lon = l.getLongitude();
				double lat = l.getLatitude();
				for (QuadRect quad : inflatedRegionQuads) {
					if (quad.contains(lon, lat, lon, lat)) {
						return true;
					}
				}
			}
			return false; // Way is outside
		} else if (entity instanceof Relation relation) {
			for (Relation.RelationMember member : relation.getMembers()) {
				if (isInsideRegionBBox(member.getEntity())) {
					return true; // recursive
				}
			}
			return false; // Relation is outside
		} else {
			return true; // default
		}
	}

}
