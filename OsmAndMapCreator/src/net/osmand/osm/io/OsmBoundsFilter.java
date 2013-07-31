package net.osmand.osm.io;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Entity.EntityId;

public class OsmBoundsFilter implements IOsmStorageFilter {
	
	private final double lonEnd;
	private final double latDown;
	private final double latUp;
	private final double lonStart;

	public OsmBoundsFilter(double latStart, double lonStart, double latEnd, double lonEnd){
		this.latUp = latStart;
		this.lonStart = lonStart;
		this.latDown = latEnd;
		this.lonEnd = lonEnd;
		
	}

	@Override
	public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
		if(entity instanceof Node){
			double lon = ((Node) entity).getLongitude();
			double lat = ((Node) entity).getLatitude();
            return latDown <= lat && lat <= latUp && lonStart <= lon && lon <= lonEnd;
        }
		// unknown for other locations
		return true;
	}

}
