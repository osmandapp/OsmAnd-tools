package net.osmand.server.assist.data;

public class LocationInfo {
	// location based
	double lat = Double.NaN;
	double lon = Double.NaN;
	double speed = Double.NaN;
	double altitude = Double.NaN;
	double hdop = Double.NaN;
	double satellites= Double.NaN;
	double azi = Double.NaN;
	// other
	double temperature = Double.NaN;
	
	String ipSender;
	
	long timestamp;
	
	public LocationInfo() {
		this(System.currentTimeMillis());
	}
	
	public LocationInfo(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public void setIpSender(String ipSender) {
		this.ipSender = ipSender;
	}
	
	public void setLatLon(double lt, double ln) {
		lat = lt;
		lon = ln;
	}
	
	public void setAltitude(double altitude) {
		this.altitude = altitude;
	}
	
	public void setAzi(double azi) {
		this.azi = azi;
	}
	
	public void setTemperature(double temperature) {
		this.temperature = temperature;
	}
	
	public void setSatellites(double satellites) {
		this.satellites = satellites;
	}
	
	public void setSpeed(double speed) {
		this.speed = speed;
	}
	
	public void setHdop(double hdop) {
		this.hdop = hdop;
	}
	
	public boolean isLocationPresent() {
		return !Double.isNaN(lat);
	}
	
	public long getTimestamp() {
		return timestamp;
	}
}
