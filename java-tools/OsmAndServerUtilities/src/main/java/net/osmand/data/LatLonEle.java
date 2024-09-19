package net.osmand.data;

public class LatLonEle extends LatLon {
	public LatLonEle(double latitude, double longitude, float elevation) {
		super(latitude, longitude);
		this.elevation = elevation;
	}

	public LatLonEle(double latitude, double longitude) {
		super(latitude, longitude);
		this.elevation = Float.NaN;
	}

	public double getElevation() {
		return elevation;
	}

	public void setElevation(float elevation) {
		this.elevation = elevation;
	}

	@Override
	public String toString() {
		return String.format("Lat %.6f Lon %.6f Ele %.2f", getLatitude(), getLongitude(), getElevation());
	}

	private float elevation;
}
