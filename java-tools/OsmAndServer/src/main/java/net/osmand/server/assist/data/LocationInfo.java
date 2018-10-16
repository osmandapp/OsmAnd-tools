package net.osmand.server.assist.data;


import javax.persistence.*;

@Embeddable
public class LocationInfo {

	// location based
	@Column(name = "lat")
	double lat = Double.NaN;

	@Column(name = "lon")
	double lon = Double.NaN;
	@Transient
	double speed = Double.NaN;
	@Transient
	double altitude = Double.NaN;
	@Transient
	double hdop = Double.NaN;
	@Transient
	double satellites= Double.NaN;
	@Transient
	double azi = Double.NaN;
	// other
	@Transient
	double temperature = Double.NaN;
	@Transient
	String ipSender;

	@Column(name = "timestamp")
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
	
	public double getLat() {
		return lat;
	}
	
	public double getLon() {
		return lon;
	}
	
}
