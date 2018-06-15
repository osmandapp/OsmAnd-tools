package net.osmand.osm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.LatLon;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RouteResultPreparation;
import net.osmand.router.RouteSegmentResult;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingContext;

import org.xmlpull.v1.XmlPullParserException;

public class DijkstraAlgorithm {

	public static void main(String[] args) throws IOException, InterruptedException, XmlPullParserException {
		File fl = new File("/Users/victorshcherb/osmand/maps/Netherlands_europe_2.obf");
		RandomAccessFile raf = new RandomAccessFile(fl, "r"); //$NON-NLS-1$ //$NON-NLS-2$
		RoutePlannerFrontEnd fe = new RoutePlannerFrontEnd(false);
		Builder builder = RoutingConfiguration.parseFromInputStream(new FileInputStream(
				"/Users/victorshcherb/osmand/repos/resources/routing/routing.xml"));
		RoutingConfiguration config = builder.build("car", RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3);
		RoutingContext ctx = fe.buildRoutingContext(config, null, new BinaryMapIndexReader[] { new BinaryMapIndexReader(raf,fl ) },
				RouteCalculationMode.NORMAL);
		RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION_TO_TEST = true;
		List<RouteSegmentResult> route = fe.searchRoute(ctx, new LatLon(52.28283, 4.8622713), new LatLon(52.326496, 4.8753176), null);
	}
}
