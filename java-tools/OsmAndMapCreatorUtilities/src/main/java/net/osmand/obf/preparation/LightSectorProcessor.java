package net.osmand.obf.preparation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.FactoryConfigurationError;

import org.apache.commons.logging.Log;

import net.osmand.IProgress;
import net.osmand.MainUtilities;
import net.osmand.PlatformUtil;
import net.osmand.binary.MapZooms;
import net.osmand.binary.MapZooms.MapZoomPair;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.obf.BinaryInspector;
import net.osmand.obf.BinaryInspector.FileExtractFrom;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import rtree.RTree;

/**
 * This is migration from SOURCE: https://github.com/quantenschaum/mapping/
 * Utility to generate a multi-level lightsector.obf file from an OSM input
 * file. This class replicates the functionality of the provided
 * 'lightsectors.py' script and the shell command sequence for generating a
 * combined OBF with 4 layers.
 */
public class LightSectorProcessor {

	private static final Log log = PlatformUtil.getLog(LightSectorProcessor.class);
	private static final AtomicLong entityIdCounter = new AtomicLong(-1000);

	// From s57.py
	private static final List<String> LEADING_LIGHT_CATEGORIES = Arrays.asList("leading", "front", "rear", "lower", "upper");

	// From s57.py, used to filter for navigational lights
	private static final Set<String> NAV_LIGHT_CATEGORIES = new HashSet<>(
			Arrays.asList("directional", "leading", "front", "rear", "lower", "upper", "moire", "bearing"));
	
	private static final String LIGHTSECTOR_TAG = "lightsector";
	private static final String LIGHTSECTOR_LOW_TAG = "lightsector:low";
	private static final String LIGHTSECTOR_SOURCE_VALUE = "source";
	private static final String LIGHTSECTOR_ORIENTATION_VALUE = "orientation";
	private static final String LIGHTSECTOR_LIMIT_VALUE = "limit";
	private static final String LIGHTSECTOR_ARC_VALUE = "arc";
	private static final String NAME_TAG = "name";
	
	private static final Set<String> LOW_LIGHT_CATEGORIES = new HashSet<>(
			Arrays.asList("fog", "low", "faint", "obscured", "part_obscured", "occasional", "not_in_use", "temporary",
					"extinguished", "existence_doubtful"));

	
	   // Abbreviation map, same as in the Python script's s57.py dependency
    private static final Map<String, String> ABBREVIATIONS = new HashMap<>();
    static {
    	ABBREVIATIONS.put("permanent", "perm");
        ABBREVIATIONS.put("occasional", "occas");
        ABBREVIATIONS.put("recommended", "rcmnd");
        ABBREVIATIONS.put("not_in_use", "unused");
        ABBREVIATIONS.put("intermittent", "interm");
        ABBREVIATIONS.put("reserved", "resvd");
        ABBREVIATIONS.put("temporary", "temp");
        ABBREVIATIONS.put("private", "priv");
        ABBREVIATIONS.put("mandatory", "mand");
        ABBREVIATIONS.put("extinguished", "exting");
        ABBREVIATIONS.put("illuminated", "illum");
        ABBREVIATIONS.put("historic", "hist");
        ABBREVIATIONS.put("public", "pub");
        ABBREVIATIONS.put("synchronized", "sync");
        ABBREVIATIONS.put("watched", "watchd");
        ABBREVIATIONS.put("unwatched", "unwtchd");
        ABBREVIATIONS.put("existence_doubtful", "ED");
        ABBREVIATIONS.put("high", "high");
        ABBREVIATIONS.put("low", "low");
        ABBREVIATIONS.put("faint", "faint");
        ABBREVIATIONS.put("intensified", "intens");
        ABBREVIATIONS.put("unintensified", "uintens");
        ABBREVIATIONS.put("restricted", "restr");
        ABBREVIATIONS.put("obscured", "obscd");
        ABBREVIATIONS.put("part_obscured", "p.obscd");
        ABBREVIATIONS.put("fog", "fog");
        // Add any other required abbreviations from s57.py here
    }

	public enum LIGHT_PROPERTY {
		SECTOR_START("sector_start", true), SECTOR_END("sector_end", true), ORIENTATION("orientation", true),
		HEIGHT("height", true), RANGE("range", true), COLOUR("colour", false), CHARACTER("character", false),
		CATEGORY("category", false), STATUS("status", false), VISIBILITY("visibility", false),
		EXHIBITION("exhibition", false), GROUP("group", false), PERIOD("period", false), SEQUENCE("sequence", false),;

		public boolean num;
		public String tag;

		public boolean isSSO() {
			return SECTOR_START == this || SECTOR_END == this || ORIENTATION == this;
		}

		public String parseValue(String vl) {
			if (!Algorithms.isEmpty(vl)) {
				if (num) {
					float v = -1;
					try {
						if (this == HEIGHT) {
							v = RouteDataObject.parseLength(vl, v);
						} else {
							v = Float.parseFloat(vl.split(" ")[0]);
						}
					} catch (NumberFormatException e) {
						System.err.printf("Error parsing %s - %s\n", tag, vl);
					}
					
					if (v < 0) {
						return null;
					}
					
				}
				return vl;
			}
			return null;
		}

		LIGHT_PROPERTY(String tag, boolean num) {
			this.tag = tag;
			this.num = num;
		}
	}

	/**
	 * Holds all configuration parameters for generating a single layer of the light
	 * sectors map. This mirrors the configuration options available in the original
	 * lightsectors.py script.
	 */
	private static class LightSectorConfigLayer {
		/** The processing level index (0-3). */
		final int level;
		/** The map zoom levels this layer applies to (e.g., "6-9;10-11"). */
		final List<MapZoomPair> mapZooms;
		/**
		 * Factor to scale the radius of the light sector arcs. Corresponds to '-a' /
		 * '--f-arc'.
		 */
		final double f_arc;
		/**
		 * Factor to scale the length of the light sector limit lines. Corresponds to
		 * '-f' / '--f-range'.
		 */
		final double f_range;
		/**
		 * The minimum nominal range a light must have to be included. Corresponds to
		 * '-r' / '--min-range'.
		 */
		final int min_range;

		// --- Parameters from python script defaults ---
		/**
		 * Seamark types to be treated as major lights (always included). Corresponds to '-M' / '--major'.
		 * "seamark:type" will be converted to light_major if present in list otherwise to light_minor
		 */
		final List<String> major_lights;
		/**
		 * The maximum rendered range for any light sector geometry. Corresponds to '-R' / '--max-range'.
		 * 
		 */
		final double max_range;
		/**
		 * Generate arcs for 360° sectors if their nominal range is >= this value.
		 * Corresponds to '-o' / '--full'.
		 */
		final double full_range_threshold;
		/**
		 * Default nominal range to assume if 'seamark:light:*:range' is not tagged.
		 * Corresponds to '--range0'.
		 */
		final double default_range;
		/**
		 * Whether to generate geometry for single-sector leading lights. Corresponds to
		 * '-l' / '--leading'.
		 */
		final boolean process_leading_lights;
		/**
		 * Whether to add all original sector data as tags to the central source node.
		 * Corresponds to '-s' / '--sector-data'.
		 */
		final boolean add_sector_data_to_source;

		LightSectorConfigLayer(int level, double f_arc, double f_range, int min_range, MapZoomPair... pairs) {
			this.level = level;
			this.mapZooms = Arrays.asList(pairs);
			this.f_arc = f_arc;
			this.f_range = f_range;
			this.min_range = min_range;

			// Default values from lightsectors.py
			this.major_lights = Arrays.asList("light_major");
			this.max_range = 12.0;
			this.full_range_threshold = 19.0;
			this.default_range = 0.0;
			this.process_leading_lights = false;
			this.add_sector_data_to_source = false;
		}
	}

	public static void main(String[] args) throws FactoryConfigurationError, Exception {
		if (args.length < 2) {
			log.error("Usage: LightSectorProcessor <input.osm/input.osm.gz> <output_directory>");
			return;
		}

		File inputFile = new File(args[0]);
		if (!inputFile.exists()) {
			log.error("Input file not found: " + inputFile.getAbsolutePath());
			return;
		}
		File outputDir = new File(".");
		if (args.length > 0) {
			outputDir = new File(args[1]);
		}
		outputDir.mkdirs();


		log.info("Starting light sectors generation...");
		String basename = inputFile.getName().substring(0, inputFile.getName().indexOf('.'));
		File out = new File(outputDir, basename + ".obf");
		new LightSectorProcessor().generateLightSectorIndex(inputFile, out, defaultLightSectorsConfig(), args);
		log.info("Successfully generated " + out.getAbsolutePath());
	}

	private static List<LightSectorConfigLayer> defaultLightSectorsConfig() {
		List<LightSectorConfigLayer> configs = new ArrayList<>();
//        configs.add(new LightSectorConfigLayer(0, 0.30, 1.6, 16, new MapZoomPair(6, 9), new MapZoomPair(10, 11))); // strangely doesn't work
        configs.add(new LightSectorConfigLayer(0, 0.30, 1.6, 16, new MapZoomPair(9, 11)));
        configs.add(new LightSectorConfigLayer(1, 0.20, 0.8, 8, new MapZoomPair(12, 13)));
        configs.add(new LightSectorConfigLayer(2, 0.15, 0.4, 4, new MapZoomPair(14, 15)));
        configs.add(new LightSectorConfigLayer(3, 0.10, 0.2, 2, new MapZoomPair(16, MapZoomPair.MAX_ALLOWED_ZOOM)));
		return configs;
	}

	/**
	 * Main method to generate the multi-level OBF index. It iterates through
	 * configurations, processing the input file for each and adding the generated
	 * data to the corresponding level in the OBF.
	 */
	public void generateLightSectorIndex(File inputFile, File outputFile,
			List<LightSectorConfigLayer> configs, String... argsGen) throws Exception {

		long start = System.currentTimeMillis();
	    InputStream is;
		if(inputFile.getName().endsWith(".gz")) {
            is = new GZIPInputStream(new FileInputStream(inputFile));
		} else {
			is = new FileInputStream(inputFile);
		}
        InputStream stream = new BufferedInputStream(is, 8192 * 4);
        OsmBaseStorage storage = new OsmBaseStorage();
        storage.parseOSM(stream, IProgress.EMPTY_PROGRESS);
        long t1 = System.currentTimeMillis() - start;
        log.info("Parse " + inputFile.getName() + ": " + ((float) (t1 / 1e3)) + " sec.");
        stream.close();
        
		List<File> fls = new ArrayList<File>();
		int ind = 0;
		for (LightSectorConfigLayer config : configs) {
			ind++;
			File osmFile = new File(outputFile.getParentFile(), outputFile.getName().replace(".obf", "_"+(ind)+".osm"));
			fls.add(new File(outputFile.getParentFile(), outputFile.getName().replace(".obf", "_"+(ind)+".obf")));
			Map<EntityId, Entity> entities = new HashMap<Entity.EntityId, Entity>();
			log.info(String.format("--- Processing for Level %d (min_range: %d, f_range: %.2f, f_arc: %.2f) ---",
					config.level, config.min_range, config.f_range, config.f_arc));
			int count = 0;
			for (Entity entity : storage.getRegisteredEntities().values()) {
				List<Entity> genEntities = processEntity(entity, config);
				if (genEntities.size() > 0) {
					count += genEntities.size();
					for (Entity e : genEntities) {
						entities.put(EntityId.valueOf(e), e);
					}
				}
				
			}
			
			log.info(String.format("-------- %d generated entities ----------", count));
			OsmStorageWriter osmStorageWriter = new OsmStorageWriter();
			
			FileOutputStream fout = new FileOutputStream(osmFile);
			osmStorageWriter.saveStorage(fout, entities, null, null, false);
			fout.close();
			
			// generate obf
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexMap = true;
			List<String> subArgs = new ArrayList<String>(Arrays.asList(argsGen));
			MainUtilities.parseIndexCreatorArgs(subArgs, settings);
			subArgs.add(0, osmFile.getAbsolutePath());
			MapZooms ms = new MapZooms();
			ms.setLevels(config.mapZooms);
			MainUtilities.generateObf(subArgs, ms, settings);
			RTree.clearCache();
			
		}
		FileExtractFrom fileExtractFrom = new FileExtractFrom();
		fileExtractFrom.from = fls;
		BinaryInspector.combineParts(outputFile, Collections.singletonList(fileExtractFrom), null);
	}

	/**
	 * Processes a single entity from the OSM file. If it's a relevant light based
	 * on the current layer's configuration, it generates the sector geometry and
	 * passes it to the BasemapProcessor.
	 */
	private List<Entity> processEntity(Entity entity, LightSectorConfigLayer config) {
		if (!(entity instanceof Node) && !(entity instanceof Way)) {
			return Collections.emptyList();
		}

		List<Map<String, String>> sectors = getSectors(entity);
		if (sectors.isEmpty()) {
			return Collections.emptyList();
		}

		String seamarkType = entity.getTag("seamark:type");
		boolean isMajorLight = config.major_lights.contains(seamarkType);
		boolean rangeSufficient = false;
		for (Map<String, String> sector : sectors) {
			try {
				double range = Double.parseDouble(sector.getOrDefault(LIGHT_PROPERTY.RANGE.tag, String.valueOf(config.default_range)));
				if (range >= config.min_range) {
					rangeSufficient = true;
					break;
				}
			} catch (NumberFormatException e) {
				System.err.printf("Error parsing range %s\n", sector.get(LIGHT_PROPERTY.RANGE.tag)); 
			}
		}

		if (isMajorLight || rangeSufficient) {
			return generateSectorsForLight(entity, sectors, config);
		}
		return Collections.emptyList();
	}

	  /**
     * Groups sectors by a key composed of their character, group, and period.
     * This is a direct, step-by-step translation of the Python group_by function.
     *
     * @param sectors The list of sector data maps.
     * @return A LinkedHashMap preserving insertion order, mapping the key to a list of sectors.
     */
    private Map<String, List<Map<String, String>>> groupSectorByCombinedKey(List<Map<String, String>> sectors) {
        // Use LinkedHashMap to preserve the order of groups as they appear, similar to Python's behavior
        Map<String, List<Map<String, String>>> grouped = new LinkedHashMap<>();

        for (Map<String, String> sector : sectors) {
            // Generate the key for grouping
            String key = sector.getOrDefault("character", "?") + "-" +
                         sector.getOrDefault("group", "?") + "-" +
                         sector.getOrDefault("period", "?");

            // Get the list for the current key, or create a new one if it doesn't exist
            List<Map<String, String>> groupList = grouped.get(key);
            if (groupList == null) {
                groupList = new ArrayList<>();
                grouped.put(key, groupList);
            }

            // Add the current sector to its group
            groupList.add(sector);
        }
        return grouped;
    }
    
    
    /**
     * Helper to format numbers like Python's nformat function: "5.0" -> "5", "5.1" -> "5.1"
     */
    private static String nformat(double v) {
        return String.format(Locale.US, "%.1f", v).replace(".0", "");
    }
    
    /**
     * Merges a list of sectors (from a single group) into a single map of properties.
     * This is a direct port of the Python `merge` function.
     */
    private Map<String, Object> mergeSectors(List<Map<String, String>> sectors) {
        Map<String, Object> merged = new HashMap<>();
        for (Map<String, String> s : sectors) {
            for (Map.Entry<String, String> entry : s.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if (v == null) continue;

                if (LIGHT_PROPERTY.RANGE.tag.equals(k) || LIGHT_PROPERTY.HEIGHT.tag.equals(k)) {
                    // Store range and height values as a list of doubles for min/max calculation later
                    @SuppressWarnings("unchecked")
					List<Double> values = (List<Double>) merged.computeIfAbsent(k, key -> new ArrayList<Double>());
                    try {
                        values.add(Double.parseDouble(v.split(" ")[0]));
                    } catch (NumberFormatException e) {
                        // Ignore if value is not a valid number
                    }
                } else if (merged.containsKey(k)) {
                    // For other string properties, combine unique values with a semicolon
                    String existing = (String) merged.get(k);
                    if (!existing.equals(v)) {
                        Set<String> values = new LinkedHashSet<>(Arrays.asList(existing.split(";")));
                        values.addAll(Arrays.asList(v.split(";")));
                        merged.put(k, String.join(";", values));
                    }
                } else {
                    merged.put(k, v);
                }
            }
        }
        return merged;
    }
    
    /**
     * Collects all unique colors from the sectors.
     */
    public static String getColors(List<Map<String, String>> sectors) {
        Set<String> colors = new TreeSet<>(Collections.reverseOrder());
        for (Map<String, String> sector : sectors) {
            String colorStr = sector.get(LIGHT_PROPERTY.COLOUR.tag);
            if (colorStr != null) {
                colors.addAll(Arrays.asList(colorStr.split(";")));
            }
        }
        return String.join(";", colors);
    }

    
    /**
     * Generates a descriptive label for a merged sector.
     * This is a complete and faithful port of the Python `label` function.
     *
     * @param sector      A map representing a merged sector's properties.
     * @param heightRange Whether to include height and range in the label.
     * @return The formatted label string.
     */
    public static String generateLabel(Map<String, ?> sector, boolean heightRange) {
        StringBuilder l = new StringBuilder();
        String sep = "\u00A0"; // Non-breaking space

        // Character
        String character = (String) sector.get(LIGHT_PROPERTY.CHARACTER.tag);
        if (character != null && !"?".equals(character)) {
            l.append(character);
        }

        // Group
        String group = (String) sector.get(LIGHT_PROPERTY.GROUP.tag);
        if (l.length() > 0 && group != null && !"?".equals(group)) {
            l.append("(").append(group).append(")");
        }

        // Colour
        String colour = (String) sector.get(LIGHT_PROPERTY.COLOUR.tag);
        if (colour != null && !"?".equals(colour)) {
            if (l.length() > 0 && !l.toString().endsWith(")")) {
                l.append(sep);
            }
            String[] colors = colour.split(";");
            Arrays.sort(colors, Collections.reverseOrder());
            for (String c : colors) {
                l.append(c.substring(0, 1).toUpperCase());
            }
        }

        // Period
        String period = (String) sector.get(LIGHT_PROPERTY.PERIOD.tag);
        if (period != null && !"?".equals(period)) {
            l.append(sep).append(period).append("s");
        }

        // Height and Range
        if (heightRange) {
            // Height
            Object heightObj = sector.get(LIGHT_PROPERTY.HEIGHT.tag);
            if (heightObj instanceof List) {
                @SuppressWarnings("unchecked")
				List<Double> heights = (List<Double>) heightObj;
                if (!heights.isEmpty()) {
                    double minH = Collections.min(heights);
                    double maxH = Collections.max(heights);
                    l.append(sep);
                    if (minH != maxH) {
                        l.append(nformat(minH)).append("-").append(nformat(maxH)).append("m");
                    } else {
                        l.append(nformat(minH)).append("m");
                    }
                }
            }

            // Range
            Object rangeObj = sector.get(LIGHT_PROPERTY.RANGE.tag);
            if (rangeObj instanceof List) {
                @SuppressWarnings("unchecked")
				List<Double> ranges = (List<Double>) rangeObj;
                if (!ranges.isEmpty()) {
                    double minR = Collections.min(ranges);
                    double maxR = Collections.max(ranges);
                    l.append(sep);
                    if (minR != maxR) {
                        l.append(nformat(minR)).append("-").append(nformat(maxR)).append("M");
                    } else {
                        l.append(nformat(minR)).append("M");
                    }
                }
            }
        }
        
        // Status, visibility, exhibition abbreviations
        List<String> misc = new ArrayList<>();
        for (String p : new String[]{LIGHT_PROPERTY.STATUS.tag, LIGHT_PROPERTY.VISIBILITY.tag, LIGHT_PROPERTY.EXHIBITION.tag}) {
            String s = (String) sector.get(p);
            if (s != null) {
                for (String part : s.split(";")) {
                    if (ABBREVIATIONS.containsKey(part)) {
                        misc.add(ABBREVIATIONS.get(part));
                    }
                }
            }
        }
        if (!misc.isEmpty()) {
            l.append(sep).append("(").append(String.join(",", misc)).append(")");
        }

        return l.toString().replace(";", "/");
    }
    
	/**
	 * It creates new Way and Node entities to represent the light sectors, arcs, and limits.
	 *
	 * @param lightSource The original OSM entity (Node or Way) for the light.
	 * @param sectors     A list of sector properties extracted from the entity's
	 *                    tags.
	 * @param config      The configuration for the current processing layer.
	 * @return A list of new Entity objects (Nodes and Ways) to be added to the map.
	 */
	private List<Entity> generateSectorsForLight(Entity lightSource, List<Map<String, String>> sectors,
			LightSectorConfigLayer config) {
		List<Entity> newEntities = new ArrayList<>();
		LatLon centerPoint = getCenterLatLon(lightSource);
		if (centerPoint == null) {
			return newEntities;
		}

	    // 1. Get name and lnam from the source entity
	    String name = lightSource.getTag("seamark:name") != null ? lightSource.getTag("seamark:name") : lightSource.getTag("name");
	    String lnam = lightSource.getTag("seamark:lnam");

	    // 2. Group sectors by their characteristics
	    Map<String, List<Map<String, String>>> groupedSectors = groupSectorByCombinedKey(sectors);

	    // 3. For each group, merge its sectors and generate a label, then join all labels
	    List<String> labels = new ArrayList<>();
	    for (List<Map<String, String>> sectorGroup : groupedSectors.values()) {
	        labels.add(generateLabel(mergeSectors(sectorGroup), true));
	    }
	    String mergedLabel = String.join(" ", labels);

	    // 4. Create the central node for the light source
	    Node centerNode = new Node(centerPoint.getLatitude(), centerPoint.getLongitude(), nextId());
	    centerNode.putTag(LIGHTSECTOR_TAG, LIGHTSECTOR_SOURCE_VALUE);
	    centerNode.putTag("seamark:type",
	            config.major_lights.contains(lightSource.getTag("seamark:type")) ? "light_major" : "light_minor");
	    
	    // 5. Set the merged/aggregated tags on the central node
	    if (name != null) centerNode.putTag("seamark:name", name);
	    if (lnam != null) centerNode.putTag("seamark:lnam", lnam);
	    centerNode.putTag("seamark:light:character", mergedLabel);
	    centerNode.putTag("seamark:light:colour", getColors(sectors));

	    newEntities.add(centerNode);

	    if (config.add_sector_data_to_source) {
	        for (int i = 0; i < sectors.size(); i++) {
	            String prefix = "seamark:light:" + (sectors.size() == 1 ? "" : (i + 1) + ":");
	            for (Map.Entry<String, String> tag : sectors.get(i).entrySet()) {
	                centerNode.putTag(prefix + tag.getKey(), tag.getValue());
	            }
	        }
	    }

	    Map<Double, Map<String, Object>> lines = new HashMap<>();
	    // This loop populates the visual elements: orientation lines, limit lines, and arcs
	    for (Map<String, String> s : sectors) {
			String sAsString = s.toString();
			boolean isLow = false;
			for (String category : LOW_LIGHT_CATEGORIES) {
			    if (!"low".equals(category)) {
			        if (sAsString.contains(category)) {
			            isLow = true;
			            break; 
			        }
			    }
			}


	        double nominalRange = config.default_range;
	        try {
	            if (s.containsKey(LIGHT_PROPERTY.RANGE.tag)) nominalRange = Double.parseDouble(s.get(LIGHT_PROPERTY.RANGE.tag));
	        } catch (NumberFormatException e) { /* use default */ }
	        nominalRange = Math.max(nominalRange, config.min_range); // r0
	        double renderedRange = Math.min(nominalRange * config.f_range, config.max_range); // r1

	        Double orientation = s.containsKey(LIGHT_PROPERTY.ORIENTATION.tag) ? Double.parseDouble(s.get(LIGHT_PROPERTY.ORIENTATION.tag)) : null;
	        Double sectorStart = s.containsKey(LIGHT_PROPERTY.SECTOR_START.tag) ? Double.parseDouble(s.get(LIGHT_PROPERTY.SECTOR_START.tag)) : null;
	        Double sectorEnd = s.containsKey(LIGHT_PROPERTY.SECTOR_END.tag) ? Double.parseDouble(s.get(LIGHT_PROPERTY.SECTOR_END.tag)) : null;

	        boolean isSector = sectorStart != null && sectorEnd != null;
	        boolean isFullCircle = isSector && (sectorStart % 360 == sectorEnd % 360);

	        // # directional line
	        if (renderedRange > 0 && orientation != null) {
	            Map<String, Object> attrs = new HashMap<>();
	            attrs.put(LIGHTSECTOR_TAG, LIGHTSECTOR_ORIENTATION_VALUE);
	            attrs.put(LIGHT_PROPERTY.ORIENTATION.tag, orientation);
	            attrs.put(LIGHT_PROPERTY.RANGE.tag, renderedRange);
	            attrs.put(LIGHT_PROPERTY.COLOUR.tag, s.get(LIGHT_PROPERTY.COLOUR.tag));
	            
	            Map<String, Object> simpleSectorObjectMap = new HashMap<>(s);
	            String nameTag = nformat(orientation) + "° " + generateLabel(simpleSectorObjectMap, false);
	            attrs.put(NAME_TAG, nameTag);
	            lines.put(orientation, attrs);
	        }
	        
	        if (sectors.size() == 1 && !config.process_leading_lights) {
				String category = s.getOrDefault(LIGHT_PROPERTY.CATEGORY.tag, "");
				boolean isLeading = false;
				for (String leadingCat : LEADING_LIGHT_CATEGORIES) {
					if (category.contains(leadingCat)) {
						isLeading = true;
						break;
					}
				}
				if (isLeading) {
					continue;
				}
			}

	        // # sector limits
	        if (renderedRange > 0 && isSector && !isFullCircle) {
				for (double bearing : new double[] { sectorStart, sectorEnd }) {
					Map<String, Object> existingLine = lines.getOrDefault(bearing, new HashMap<>());
					double existingRange = existingLine.get(LIGHT_PROPERTY.RANGE.tag) instanceof Number
							? ((Number) existingLine.get(LIGHT_PROPERTY.RANGE.tag)).doubleValue() : 0.0;
					if (existingRange < renderedRange
							&& !LIGHTSECTOR_ORIENTATION_VALUE.equals(existingLine.get(LIGHTSECTOR_TAG))) {
						Map<String, Object> attrs = new HashMap<>();
						attrs.put(LIGHTSECTOR_TAG, LIGHTSECTOR_LIMIT_VALUE);
						attrs.put(LIGHT_PROPERTY.ORIENTATION.tag, bearing);
						attrs.put(LIGHT_PROPERTY.RANGE.tag, renderedRange);
						attrs.put(NAME_TAG, nformat(bearing) + "°");
	                    lines.put(bearing, attrs);
	                }
	            }
	        }

	        // # sector arc
	        if (renderedRange > 0 && isSector && (!isFullCircle || nominalRange >= config.full_range_threshold)) {
	            double start = sectorStart;
	            double end = sectorEnd;
	            if (start >= end) end += 360;

	            double arcRadius = renderedRange * config.f_arc;
	            int pointsCount = Math.max(3, (int) Math.ceil(Math.abs(end - start) / 5.0));

	            Way arcWay = new Way(nextId());
	            for (int i = 0; i <= pointsCount; i++) {
	                double bearing = start + (i * (end - start) / pointsCount);
	                LatLon arcPoint = MapUtils.rhumbDestinationPoint(centerPoint, arcRadius * 1852, bearing);
	                Node arcNode = new Node(arcPoint.getLatitude(), arcPoint.getLongitude(), nextId());
	                arcWay.addNode(arcNode);
	                newEntities.add(arcNode);
	            }
	            
	            arcWay.putTag(LIGHTSECTOR_TAG, LIGHTSECTOR_ARC_VALUE);
				if (s.get(LIGHT_PROPERTY.COLOUR.tag) != null) {
					arcWay.putTag(LIGHT_PROPERTY.COLOUR.tag, s.get(LIGHT_PROPERTY.COLOUR.tag));
				}
	            arcWay.putTag(NAME_TAG, generateLabel(s, false));
				if (isLow) {
					arcWay.putTag(LIGHTSECTOR_LOW_TAG, "yes");
				}
	            newEntities.add(arcWay);
	        }
	    }

	    for (Map.Entry<Double, Map<String, Object>> entry : lines.entrySet()) {
	        Map<String, Object> attrs = entry.getValue();
	        double bearing = (Double) attrs.get(LIGHT_PROPERTY.ORIENTATION.tag);
	        double range = (Double) attrs.get(LIGHT_PROPERTY.RANGE.tag);

	        LatLon endPoint = MapUtils.rhumbDestinationPoint(centerPoint, range * 1852, bearing);
	        Node endNode = new Node(endPoint.getLatitude(), endPoint.getLongitude(), nextId());
	        Way lineWay = new Way(nextId());
	        lineWay.addNode(centerNode);
	        lineWay.addNode(endNode);

	        for (Map.Entry<String, Object> tag : attrs.entrySet()) {
	            if (tag.getValue() != null) {
	                lineWay.putTag(tag.getKey(), String.valueOf(tag.getValue()));
	            }
	        }
	        newEntities.add(endNode);
	        newEntities.add(lineWay);
	    }

		return newEntities;
	}

	/**
	 * Extracts light sector information from an entity's tags. It handles both
	 * single lights (seamark:light:*) and multiple lights on one feature
	 * (seamark:light:1:*, seamark:light:2:*).
	 */
	private static List<Map<String, String>> getSectors(Entity entity) {
		List<Map<String, String>> sectors = new ArrayList<>();
		Map<String, String> allTags = entity.getTags();

		for (int i = 0; i < 50; i++) { // Corresponds to `for i in range(50):`
			Map<String, String> currentSector = new HashMap<>();
			String prefix = "seamark:light:" + (i == 0 ? "" : i + ":");

			boolean hasSSO = false;
			for (LIGHT_PROPERTY l : LIGHT_PROPERTY.values()) {
				String vl = l.parseValue(allTags.get(prefix + l.tag));
				if (vl != null) {
					if (l.isSSO()) {
						hasSSO = true;
					}
					currentSector.put(l.tag, vl);
				}
			}

			// `if not s and i > 0: break`
			if (currentSector.isEmpty() && i > 0) {
				break;
			}
			if (currentSector.isEmpty()) {
				continue;
			}

			// if the light has a category, and it does not intersect with the navigational categories, skip it.
			String category = currentSector.get("category");
			if (category != null) {
				Set<String> cats = new HashSet<>(Arrays.asList(category.split(";")));
				cats.retainAll(NAV_LIGHT_CATEGORIES);
				if (cats.isEmpty()) {
					continue; // Skip if there is no intersection with navigational categories.
				}
			}

			// `if s and all(x not in s for x in SSO): s["sector_start"] = 0;
			// s["sector_end"] = 360`
			if (!hasSSO) {
				currentSector.put(LIGHT_PROPERTY.SECTOR_START.tag, "0");
				currentSector.put(LIGHT_PROPERTY.SECTOR_END.tag, "360");
			}
			sectors.add(currentSector);
		}
		return sectors;
	}

	/**
	 * Gets the geographical center of an entity (either a Node's location or a
	 * Way's center).
	 */
	private static LatLon getCenterLatLon(Entity entity) {
		return OsmMapUtils.getCenter(entity);
	}

	/**
	 * Generates a unique, negative ID for new entities.
	 */
	private static long nextId() {
		return entityIdCounter.getAndDecrement();
	}
}
