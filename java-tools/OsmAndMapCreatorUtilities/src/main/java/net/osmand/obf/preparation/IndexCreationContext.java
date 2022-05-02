package net.osmand.obf.preparation;

import net.osmand.binary.Abbreviations;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.util.Algorithms;
import net.osmand.util.translit.ChineseTranslitHelper;
import net.osmand.util.translit.JapaneseTranslitHelper;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class IndexCreationContext {
    private static final Log log = LogFactory.getLog(IndexCreationContext.class);
    private static final String JAPAN = "japan";
	private static final String CHINA = "china";

    public OsmandRegions allRegions;
    public boolean basemap;

    private boolean decryptAbbreviations = false;
    private boolean translitJapaneseNames = false;
	private boolean translitChineseNames = false;
	private IndexCreator indexCreator;

	IndexCreationContext(IndexCreator indexCreator, String regionName, boolean basemap) {
		this.indexCreator = indexCreator;
		this.allRegions = prepareRegions();
		this.basemap = basemap;
		if (regionName != null) {
			this.translitJapaneseNames = regionName.toLowerCase().startsWith(JAPAN);
			this.translitChineseNames = regionName.toLowerCase().startsWith(CHINA);
			this.decryptAbbreviations = needDecryptAbbreviations(getRegionLang(allRegions, regionName));
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
	
	public void translitJapaneseNames(Entity e, boolean addRegionTag) {
		if (needTranslitName(e, addRegionTag, translitJapaneseNames, JAPAN)) {
			e.putTag(OSMTagKey.NAME_EN.getValue(),
					JapaneseTranslitHelper.getEnglishTransliteration(e.getTag(OSMTagKey.NAME.getValue())));
		}
	}
	
	public void translitChineseNames(Entity e, boolean addRegionTag) {
		if (needTranslitName(e, addRegionTag, translitChineseNames, CHINA)) {
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
	
	private boolean needTranslitName(Entity e, boolean addRegionTag, boolean translitByRegionName, String region) {
		if (!Algorithms.isEmpty(e.getTag(OSMTagKey.NAME_EN.getValue()))
				|| Algorithms.isEmpty(e.getTag(OSMTagKey.NAME.getValue()))) {
			return false;
		}
		if (translitByRegionName) {
			return true;
		} else if (addRegionTag) {
			for (String s : calcRegionTag(e, false)) {
				if (s.toLowerCase().startsWith(region)) {
					return true;
				}
			}
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

	
}
