package net.osmand.render;

import net.osmand.MainUtilities;
import net.osmand.util.Algorithms;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public class GenerateMvtIcons {

	public static void main(String[] args) throws Exception {
		MainUtilities.CommandLineOpts opts = new MainUtilities.CommandLineOpts(args);
		if (opts.getBoolean("--help")) {
			printHelp();
			System.exit(0);
		}

		String outputFolder = opts.getStrings().isEmpty() ? null : opts.getStrings().get(0);
		String repoDir = System.getenv("repo_dir");
		if (outputFolder == null || Algorithms.isEmpty(repoDir)) {
			printHelp();
			return;
		}

		int shieldSize = opts.getIntOrDefault("--shield-size", SvgMapLegendGenerator.defaultShieldSize);
		String styleFilePath = opts.getOpt("--style");
		if (styleFilePath == null) {
			styleFilePath = repoDir + "/resources/rendering_styles/default.render.xml";
		}

		RenderingRulesStorage storage = RenderingRulesStorage.getTestStorageForStyle(styleFilePath);
		Set<IconInfo> icons = new GenerateMvtIcons().extractAllIcons(storage);
		System.out.println("Total icons: " + icons.size());

		File outDir = new File(outputFolder);
		outDir.mkdirs();

		SvgMapLegendGenerator.canvasWidth = shieldSize;
		SvgMapLegendGenerator.canvasHeight = shieldSize;

		int generated = 0;
		int failed = 0;
		for (IconInfo iconInfo : icons) {
			try {
				String svg = SvgMapLegendGenerator.SvgGenerator.generate(
						iconInfo.icon,
						SvgMapLegendGenerator.defaultIconSize,
						iconInfo.shield,
						shieldSize,
						null, 0);
				String fileName = iconInfo.icon + ".svg";
				try (FileWriter writer = new FileWriter(new File(outDir, fileName), StandardCharsets.UTF_8)) {
					writer.write(svg);
				}
				generated++;
			} catch (Exception e) {
				System.err.println("SKIP " + iconInfo.icon + ": " + e.getMessage());
				failed++;
			}
		}
		System.out.println("Generated: " + generated + ", skipped: " + failed + " -> " + outputFolder);
	}

	private static void printHelp() {
		System.out.println("""
			Set 'repo_dir' env variable pointing to the OsmAnd repository root
			Usage: generate-mvt-icons <output-folder> [--style=path-to-render.xml] [--shield-size=40]
			--style path-to-render.xml by default: $repo_dir + "/resources/rendering_styles/default.render.xml"
			""");
	}

	private final Set<IconInfo> allIcons = new LinkedHashSet<>();

	public static class IconInfo {
		public String tag;
		public String value;
		public String icon;
		public String shield;
		public String source; // "POINT_RULES"

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof IconInfo)) {
				return false;
			}
			IconInfo that = (IconInfo) o;
			return Objects.equals(icon, that.icon)
					&& Objects.equals(shield, that.shield);
		}

		@Override
		public int hashCode() {
			return Objects.hash(icon, shield);
		}

		@Override
		public String toString() {
			return String.format("Tag: %s, Value: %s, Icon: %s, Shield: %s (Source: %s)",
					tag, value, icon, shield, source);
		}
	}

	public Set<IconInfo> extractAllIcons(RenderingRulesStorage storage) {
		allIcons.clear();

		extractFromRuleType(storage, RenderingRulesStorage.POINT_RULES, "POINT_RULES");

		return allIcons;
	}

	private void extractFromRuleType(RenderingRulesStorage storage, int ruleType, String source) {
		RenderingRule[] rules = storage.getRules(ruleType);

		for (int i = 0; i < rules.length; i++) {
			int key = storage.getRuleTagValueKey(ruleType, i);
			String tag = storage.getTagString(key);
			String value = storage.getValueString(key);

			RenderingRule rule = rules[i];
			extractIconsFromRule(rule, tag, value, source, null);
		}
	}

	private IconInfo extractIconsFromRule(RenderingRule rule, String tag, String value, String source, IconInfo parent) {
		if (rule.getIntPropertyValue("nightMode") == 1 || rule.getIntPropertyValue("streetLighting") == 1) {
			return null;
		}
		IconInfo iconInfo = extractIconInfo(rule, parent);
		if (iconInfo != null && !Algorithms.isEmpty(iconInfo.icon)) {
			iconInfo.tag = tag;
			iconInfo.value = value;
			iconInfo.source = source;
			allIcons.add(iconInfo);
		}

		IconInfo current = iconInfo != null ? iconInfo : parent;

		extractAdditionalIcons(rule, tag, value, source);

		// ifChildren (apply/apply_if) accumulate state sequentially — each sibling
		// inherits from the previous one, mirroring how the renderer applies them.
		IconInfo accumulated = current;
		for (RenderingRule child : rule.getIfChildren()) {
			IconInfo contribution = extractIconsFromRule(child, tag, value, source + "/if", accumulated);
			if (contribution != null) {
				accumulated = contribution;
			}
		}

		for (RenderingRule child : rule.getIfElseChildren()) {
			extractIconsFromRule(child, tag, value, source + "/ifElse", current);
		}
		return iconInfo;
	}

	private IconInfo extractIconInfo(RenderingRule rule, IconInfo parent) {
		String icon = rule.getStringPropertyValue("icon");
		String shield = rule.getStringPropertyValue("shield");

		if (!Algorithms.isEmpty(icon) || shield != null) {
			IconInfo info = new IconInfo();
			info.icon = !Algorithms.isEmpty(icon) ? icon : (parent != null ? parent.icon : null);
			info.shield = shield != null ? (shield.isEmpty() ? null : shield) : (parent != null ? parent.shield : null);
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
