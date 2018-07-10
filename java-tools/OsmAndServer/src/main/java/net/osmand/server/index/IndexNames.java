package net.osmand.server.index;

import java.util.Set;
import java.util.TreeSet;

public class IndexNames {

    private static final Set<String> INDEX_NAME_SET = new TreeSet<>();

    static {
        INDEX_NAME_SET.add("region");
        INDEX_NAME_SET.add("road_region");
        INDEX_NAME_SET.add("srtmcountry");
        INDEX_NAME_SET.add("wiki");
        INDEX_NAME_SET.add("wikivoyage");
    }

    public static final String REGION = "region";
    public static final String ROAD_REGION = "road_region";
    public static final String SRTMCOUNTRY = "srtmcountry";
    public static final String WIKI = "wiki";
    public static final String WIKIVOYAGE = "wikivoyage";


    public static Set<String> getNames() {
        return INDEX_NAME_SET;
    }
}
