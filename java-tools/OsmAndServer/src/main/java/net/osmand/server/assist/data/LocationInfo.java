package net.osmand.server.assist.data;


import javax.persistence.*;


@Table(name = "telegram_location_info")
@Entity(name = "LocationInfo")
public class LocationInfo {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "id")
	long id;
	// location based
	@Column(name = "lat")
	double lat = Double.NaN;

	@Column(name = "lon")
	double lon = Double.NaN;
	@Column(name = "speed")
	double speed = Double.NaN;
	@Column(name = "altitude")
	double altitude = Double.NaN;
	@Column(name = "hdop")
	double hdop = Double.NaN;
	@Column(name = "satellites")
	double satellites= Double.NaN;
	@Column(name = "azi")
	double azi = Double.NaN;
	// other
	@Column(name = "temperature")
	double temperature = Double.NaN;

	@Column(name = "ip_sender")
	String ipSender;

	@Column(name = "tmstmp")
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
