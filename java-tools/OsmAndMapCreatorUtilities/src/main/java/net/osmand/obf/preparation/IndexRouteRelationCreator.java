package net.osmand.obf.preparation;

import net.osmand.data.Amenity;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.*;

import static net.osmand.obf.OsmGpxWriteContext.ROUTE_ID_TAG;

public class IndexRouteRelationCreator {
	private static final String[] FILTERED_TAGS = {
			"hiking", // 244k
			"bicycle", // 119k
			"foot", // 63k
			"mtb", // 29k
			"piste", // 14k
			"ski", // 8k
			"horse", // 4k
			"running", // 1k
			"snowmobile", // 1k
			"fitness_trail", // 1k
			"canoe", // 0.8k
			"canyoning", // 0.6k
			"motorboat", // 0.4k
			"boat", // 0.3k
			"waterway", // 0.3k
			"inline_skates", // 0.2k
			"via_ferrata", // 0.2k
			"walking", // 0.2k
			"ferrata", // proposed
			// Ignored: bus detour emergency_access evacuation ferry funicular historic light_rail motorcycle
			// Ignored: power railway road share_taxi subway taxi tracks train tram transhumance trolleybus worship
	};

	private static final Log log = LogFactory.getLog(IndexRouteRelationCreatorOld.class);

	private final IndexCreatorSettings settings;
	private final IndexPoiCreator indexPoiCreator;
	private final IndexVectorMapCreator indexMapCreator;

	private final RelationTagsPropagation transformer;
	private final MapRenderingTypesEncoder renderingTypes;

	public IndexRouteRelationCreator(@Nonnull IndexCreatorSettings settings,
	                                 @Nonnull IndexPoiCreator indexPoiCreator,
	                                 @Nonnull IndexVectorMapCreator indexMapCreator) {
		this.settings = settings;
		this.indexPoiCreator = indexPoiCreator;
		this.indexMapCreator = indexMapCreator;
		this.transformer = indexMapCreator.tagsTransformer;
		this.renderingTypes = indexMapCreator.renderingTypes;
	}

	// based on RouteRelationExtractor.saveGpx()
	public void iterateRelation(Relation relation, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		if ("route".equals(relation.getTag("type")) && isSupportedRouteType(relation.getTag("route"))) {
			Map<String, String> tags = new LinkedHashMap<>(relation.getTags());

			tags.put("width", "roadstyle");
			tags.put("translucent_line_colors", "yes");
			tags.put(ROUTE_ID_TAG, Amenity.ROUTE_ID_OSM_PREFIX + relation.getId());

			List<Way> waysToJoin = new ArrayList<>();
			relation.getMembers();
			for (Relation.RelationMember member : relation.getMembers()) {
				if (member.getEntity() instanceof Way way) {
					if ("yes".equals(way.getTag("area"))) {
						continue; // skip (eg https://www.openstreetmap.org/way/746544031)
					}
					waysToJoin.add(way);
					transformer.addPropogatedTags(renderingTypes,
							MapRenderingTypesEncoder.EntityConvertApplyType.MAP, way, way.getModifiableTags());
					tags.putAll(getShieldTagsFromOsmcTags(way.getTags()));
				}
			}

			if (relation.getId() == 8240320) {
				System.err.printf("WARN: XXX relation (%s)\n", relation);
				for (Relation.RelationMember member : relation.getMembers()) {
					indexMapCreator.iterateMainEntity(member.getEntity(), ctx, icc);
				}
			}
		}
	}

	public static boolean isSupportedRouteType(@Nullable String routeType) {
		if (routeType != null) {
			for (String tag : routeType.split("[;, ]")) {
				for (String value : IndexRouteRelationCreator.FILTERED_TAGS) {
					if (tag.startsWith(value) || tag.endsWith(value)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private static final Map<String, String> OSMC_TAGS_TO_SHIELD_PROPS = Map.of(
			"osmc_text", "shield_text",
			"osmc_background", "shield_bg",
			"osmc_foreground", "shield_fg",
			"osmc_foreground2", "shield_fg_2",
			"osmc_textcolor", "shield_textcolor",
			"osmc_waycolor", "shield_waycolor" // waycolor is a part of osmc:symbol and must be applied to whole way
	);

	private static final String OSMC_ICON_PREFIX = "osmc_";
	private static final String OSMC_ICON_BG_SUFFIX = "_bg";
	private static final Set<String> SHIELD_BG_ICONS = Set.of("shield_bg");
	private static final Set<String> SHIELD_FG_ICONS = Set.of("shield_fg", "sheld_fg_2");

	@Nonnull
	public static Map<String, String> getShieldTagsFromOsmcTags(@Nonnull Map<String, String> tags) {
		Map<String, String> result = new LinkedHashMap<>();
		for (String tag : tags.keySet()) {
			for (String match : OSMC_TAGS_TO_SHIELD_PROPS.keySet()) {
				if (tag.endsWith(match)) {
					final String key = OSMC_TAGS_TO_SHIELD_PROPS.get(match);
					final String prefix =
							(SHIELD_BG_ICONS.contains(key) || SHIELD_FG_ICONS.contains(key)) ? OSMC_ICON_PREFIX : "";
					final String suffix = SHIELD_BG_ICONS.contains(key) ? OSMC_ICON_BG_SUFFIX : "";
					final String val = prefix + tags.get(tag) + suffix;
					result.putIfAbsent(key, val); // prefer 1st
				}
			}
		}
		return result;
	}
}
