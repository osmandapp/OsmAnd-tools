package net.osmand.data;

import net.osmand.data.City.CityType;

public class Boundary {
	
	private long boundaryId;
	private String name;
	private String altName;
	private int adminLevel;
	
	private long adminCenterId;
	private City city;
	private CityType cityType;
	private Multipolygon multipolygon;
	
	public Boundary(MultipolygonBuilder m) {
		multipolygon = m.build();
	}
	
	public boolean containsPoint(double latitude, double longitude) {
		return multipolygon.containsPoint(latitude, longitude);
	}
	
	public void mergeWith(Boundary boundary) {
		multipolygon.mergeWith(boundary.multipolygon);
	}
	
	public boolean containsPoint(LatLon location) {
		return multipolygon.containsPoint(location);
	}
	
	public long getBoundaryId() {
		return boundaryId;
	}
	
	public void setBoundaryId(long boundaryId) {
		this.boundaryId = boundaryId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public void setAltName(String altName) {
		this.altName = altName;
	}
	
	public String getAltName() {
		return altName;
	}

	public int getAdminLevel() {
		return adminLevel;
	}
	
	public boolean hasAdminLevel(){
		return adminLevel > 0;
	}

	public void setAdminLevel(int adminLevel) {
		this.adminLevel = adminLevel;
	}
	
	@Override
	public String toString() {
		return  getName() + " alevel:" + getAdminLevel() + " type: has opened polygons:" + 
				multipolygon.hasOpenedPolygons() + " no. of outer polygons:" +multipolygon.countOuterPolygons();
	}
	

	public void setAdminCenterId(long l) {
		this.adminCenterId = l;
	}
	
	public boolean hasAdminCenterId(){
		return adminCenterId != 0;
	}
	
	public long getAdminCenterId() {
		return adminCenterId;
	}

	public void setCityType(CityType cityType) {
		this.cityType = cityType;
	}
	
	public CityType getCityType() {
		return cityType;
	}

	public LatLon getCenterPoint() {
		return multipolygon.getCenterPoint();
	}

	public City getCity() {
		return city;
	}

	public void setCity(City city) {
		this.city = city;
		this.cityType = city.getType();
	}

}
