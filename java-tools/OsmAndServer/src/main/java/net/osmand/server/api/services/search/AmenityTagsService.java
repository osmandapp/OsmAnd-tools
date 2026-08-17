package net.osmand.server.api.services.search;

import net.osmand.data.AdditionalInfoBundle;
import net.osmand.data.Amenity;
import net.osmand.data.AmenityTagEntry;
import net.osmand.data.AmenityTagEntriesBuilder;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiType;
import net.osmand.util.Algorithms;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class AmenityTagsService {

	private final PoiTypesService poiTypesService;

	public AmenityTagsService(PoiTypesService poiTypesService) {
		this.poiTypesService = poiTypesService;
	}

	public List<VisibleTag> convertToVisibleTags(Map<String, String> tags, String lang) {
		if (tags == null || tags.isEmpty()) {
			return Collections.emptyList();
		}
		AdditionalInfoBundle infoBundle = new AdditionalInfoBundle(poiTypesService.getMapPoiTypes(lang), tags);
		List<String> preferredLangs = lang != null ? List.of(lang) : List.of();
		boolean allowNoteTag = false; // The "note" tag is enabled only for OSM editing.
		List<AmenityTagEntry> tagEntries = infoBundle.getVisibleTags(allowNoteTag, preferredLangs);

		List<AmenityTagEntry> infoTagEntries = new ArrayList<>();
		List<AmenityTagEntry> descriptionTagEntries = new ArrayList<>();
		for (AmenityTagEntry tagEntry : tagEntries) {
			if (tagEntry.isDescription) {
				descriptionTagEntries.add(tagEntry);
			} else {
				infoTagEntries.add(tagEntry);
			}
		}
		AmenityTagEntriesBuilder.sortDescriptionEntries(descriptionTagEntries, lang);

		List<AmenityTagEntry> sortedTagEntries = sortTagEntries(infoBundle, infoTagEntries);
		sortedTagEntries.addAll(descriptionTagEntries);
		return toVisibleTags(sortedTagEntries);
	}

	private List<AmenityTagEntry> sortTagEntries(AdditionalInfoBundle infoBundle, List<AmenityTagEntry> tagEntries) {
		PoiCategory category = infoBundle.getCategory();
		List<AmenityTagEntry> namedTagEntries = new ArrayList<>();
		for (AmenityTagEntry tagEntry : tagEntries) {
			namedTagEntries.add(buildWithName(tagEntry, resolveSortName(infoBundle, category, tagEntry)));
		}
		AmenityTagEntriesBuilder.sortInfoEntries(namedTagEntries);
		return namedTagEntries;
	}

	private String resolveSortName(AdditionalInfoBundle infoBundle, PoiCategory category, AmenityTagEntry tagEntry) {
		if (tagEntry.collapsableEntryType == AmenityTagEntry.CollapsableEntryType.POI_TYPE_GROUP) {
			return resolveGroupSortName(tagEntry);
		}
		PoiType additionalType = infoBundle.resolvePoiType(category, tagEntry.key, tagEntry.value).additionalType();
		return additionalType != null ? additionalType.getKeyName() : tagEntry.key;
	}

	private String resolveGroupSortName(AmenityTagEntry tagEntry) {
		List<PoiType> types = tagEntry.collapsablePoiTypes;
		if (Algorithms.isEmpty(types)) {
			return tagEntry.key;
		}
		if (tagEntry.poiAdditional) {
			return types.get(0).getKeyName();
		}
		return types.get(0).getCategory().getKeyName();
	}

	private AmenityTagEntry buildWithName(AmenityTagEntry tagEntry, String name) {
		return AmenityTagEntry.Builder.from(tagEntry).setName(name).build();
	}

	private String joinPoiTypeKeys(AmenityTagEntry tagEntry) {
		return tagEntry.collapsablePoiTypes.stream()
				.map(PoiType::getKeyName)
				.collect(Collectors.joining(Amenity.SEPARATOR));
	}

	private List<VisibleTag> toVisibleTags(List<AmenityTagEntry> tagEntries) {
		List<VisibleTag> result = new ArrayList<>();
		for (AmenityTagEntry tagEntry : tagEntries) {
			VisibleTag tag = switch (tagEntry.collapsableEntryType) {
				case POI_TYPE_GROUP -> toGroupTag(tagEntry);
				case PLAIN -> toLocalizedTag(tagEntry);
				case NONE -> toPlainTag(tagEntry);
				case ELEVATION_PILLS, OPENING_HOURS -> throw new UnsupportedOperationException(
						"AmenityTagEntry.CollapsableEntryType." + tagEntry.collapsableEntryType
								+ " is a UI-only rendering hint with no server-side tag representation");
			};
			if (tag != null) {
				result.add(tag);
			}
		}
		return result;
	}

	private VisibleTag toGroupTag(AmenityTagEntry tagEntry) {
		String key = Amenity.COLLAPSABLE_PREFIX + tagEntry.key;
		return new VisibleTag(key, joinPoiTypeKeys(tagEntry), null);
	}

	private VisibleTag toPlainTag(AmenityTagEntry tagEntry) {
		return tagEntry.value != null ? new VisibleTag(tagEntry.key, tagEntry.value, null) : null;
	}

	private VisibleTag toLocalizedTag(AmenityTagEntry tagEntry) {
		LangValue mainValue = toLangValue(tagEntry);
		List<LangValue> otherLangs = tagEntry.collapsableEntries.stream()
				.map(this::toLangValue)
				.collect(Collectors.toList());
		int idx = tagEntry.key.indexOf(':');
		String mainKey = idx >= 0 ? tagEntry.key.substring(0, idx) : tagEntry.key;
		return new VisibleTag(mainKey, mainValue.value(), mainValue.lang(),
				otherLangs.isEmpty() ? null : otherLangs);
	}

	private LangValue toLangValue(AmenityTagEntry child) {
		int idx = child.key.indexOf(':');
		return idx >= 0
				? new LangValue(child.value, child.key.substring(idx + 1))
				: new LangValue(child.value, null);
	}

	public record VisibleTag(String key, String value, String lang, List<LangValue> otherLangs) {
		public VisibleTag(String key, String value, String lang) {
			this(key, value, lang, null);
		}
	}

	public record LangValue(String value, String lang) {
	}
}
