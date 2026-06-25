package net.osmand.render;

import net.osmand.util.Algorithms;

import java.util.HashSet;
import java.util.Set;

public class IconGenerator {

	public static void main(String[] args) throws Exception {
		String renderFilePath;
		if (args.length > 0) {
			renderFilePath = args[0];
		} else {
			String repoDir = System.getenv("repo_dir");
			if (Algorithms.isEmpty(repoDir)) {
				throw new IllegalArgumentException("Usage: IconGenerator <path-to-render.xml>\n" +
						"Or set 'repo_dir' env variable pointing to resources repository.");
			}
			String style = args.length > 1 ? args[1] : "default";
			renderFilePath = repoDir + "/resources/rendering_styles/" + style + ".render.xml";
		}
		RenderingRulesStorage storage = RenderingRulesStorage.getTestStorageForStyle(renderFilePath);
		Set<IconInfo> icons = new IconGenerator().extractAllIcons(storage);
		System.out.println("Total icons: " + icons.size());
		for (IconInfo iconInfo : icons) {
			System.out.println(iconInfo);
		}
	}

	private Set<IconInfo> allIcons = new HashSet<>();

	public static class IconInfo {
		public String tag;
		public String value;
		public String icon;
		public String shield;
		public Float iconVisibleSize;
		public String source; // "POINT_RULES"

		@Override
		public String toString() {
			return String.format("Tag: %s, Value: %s, Icon: %s, Shield: %s, Size: %s (Source: %s)",
					tag, value, icon, shield, iconVisibleSize, source);
		}
	}

	public Set<IconInfo> extractAllIcons(RenderingRulesStorage storage) {
		allIcons.clear();

		extractFromRuleType(storage, RenderingRulesStorage.POINT_RULES, "POINT_RULES");
//		extractFromRuleType(storage, RenderingRulesStorage.LINE_RULES, "LINE_RULES");
//		extractFromRuleType(storage, RenderingRulesStorage.POLYGON_RULES, "POLYGON_RULES");
//		extractFromRuleType(storage, RenderingRulesStorage.TEXT_RULES, "TEXT_RULES");

		return allIcons;
	}

	private void extractFromRuleType(RenderingRulesStorage storage, int ruleType, String source) {
		RenderingRule[] rules = storage.getRules(ruleType);

		for (int i = 0; i < rules.length; i++) {
			int key = storage.getRuleTagValueKey(ruleType, i);
			String tag = storage.getTagString(key);
			String value = storage.getValueString(key);

			RenderingRule rule = rules[i];
			extractIconsFromRule(rule, tag, value, source);
		}
	}

	private void extractIconsFromRule(RenderingRule rule, String tag, String value, String source) {
		IconInfo iconInfo = extractIconInfo(rule);
		if (iconInfo != null) {
			iconInfo.tag = tag;
			iconInfo.value = value;
			iconInfo.source = source;
			allIcons.add(iconInfo);
		}

		extractAdditionalIcons(rule, tag, value, source);

		for (RenderingRule child : rule.getIfChildren()) {
			extractIconsFromRule(child, tag, value, source + "/if");
		}

		for (RenderingRule child : rule.getIfElseChildren()) {
			extractIconsFromRule(child, tag, value, source + "/ifElse");
		}
	}

	private IconInfo extractIconInfo(RenderingRule rule) {
		String icon = rule.getStringPropertyValue("icon");
		String shield = rule.getStringPropertyValue("shield");
		float iconVisibleSize = rule.getFloatPropertyValue("iconVisibleSize");

		if (!Algorithms.isEmpty(icon)) {
			IconInfo info = new IconInfo();
			info.icon = icon;
			info.shield = shield;
			info.iconVisibleSize = iconVisibleSize > 0 ? iconVisibleSize : null;
			return info;
		}
		return null;
	}

	private void extractAdditionalIcons(RenderingRule rule, String tag, String value, String source) {
		String[] additionalIconProps = {"icon__1", "icon_2", "icon_3", "icon_4", "icon_5"};

		for (String iconProp : additionalIconProps) {
			String icon = rule.getStringPropertyValue(iconProp);
			if (icon != null && !icon.isEmpty()) {
				IconInfo info = new IconInfo();
				info.tag = tag;
				info.value = value;
				info.icon = icon;
				info.source = source + "/" + iconProp;
				allIcons.add(info);
			}
		}
	}
}
