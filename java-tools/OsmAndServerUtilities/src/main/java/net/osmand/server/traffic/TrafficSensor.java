package net.osmand.server.traffic;

import java.util.Arrays;

/**
 * One traffic measurement point with a day of hourly readings.
 * Missing values are NaN.
 */
public class TrafficSensor {

	public static final int HOURS = 24;

	public static final int STATE_UNKNOWN = 0;
	public static final int STATE_FLUID = 1;
	public static final int STATE_PRE_SATURATED = 2;
	public static final int STATE_SATURATED = 3;
	public static final int STATE_BLOCKED = 4;

	public static final double FAULT_MAX_FLOW = 10; // veh/h
	public static final double FAULT_MIN_OCCUPANCY = 50; // %

	public final String id;
	public String name = "";
	public String from = "";
	public String to = "";
	public String type = "";

	// display geometry; for directional sensors it follows the direction of travel
	public double[] lat;
	public double[] lon;
	public boolean directional;
	// heading of travel in degrees when the source gives it (NaN otherwise), and how far the road may turn away from it
	public double heading = Double.NaN;
	public double headingTolerance = 60;

	public final double[] flow = nan();          // vehicles per hour
	public final double[] occupancy = nan();     // percent of the hour the loop is occupied
	public final double[] measuredSpeed = nan(); // km/h
	public final double[] estimatedSpeed = nan();// km/h
	public final int[] state = new int[HOURS];
	public final int[] roadState = new int[HOURS]; // 0 unknown, 1 open, 2 closed, 3 invalid

	public ObfRoadMatcher.RoadMatch match;
	public double freeFlowSpeed = Double.NaN;    // km/h
	public boolean freeFlowFromMap;
	public double effectiveLength = Double.NaN;  // km, see LoopSpeedEstimator
	public boolean effectiveLengthFallback;

	public TrafficSensor(String id) {
		this.id = id;
	}

	public boolean hasMeasuredSpeed() {
		for (double v : measuredSpeed) {
			if (v > 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * The loop is occupied but almost nothing passes: a vehicle parked on it or a stuck detector
	 * (Madrid 10 Jun 2026: 33 of 1,817 blocked hours on 5 detectors; none in Paris 9 Sep 2026).
	 */
	public boolean isFaulty(int hour) {
		return flow[hour] <= FAULT_MAX_FLOW && occupancy[hour] >= FAULT_MIN_OCCUPANCY;
	}

	public double speed(int hour) {
		return measuredSpeed[hour] > 0 ? measuredSpeed[hour] : estimatedSpeed[hour];
	}

	// several readings per hour (15-minute history, live snapshots) are averaged into the hour
	private double[][] samples;

	/**
	 * @param flow veh/h or NaN, occupancy % or NaN, speed km/h (<= 0 unknown), state 1-4 (0 unknown)
	 */
	public void addSample(int hour, double flow, double occupancy, double speed, int state) {
		if (samples == null) {
			samples = new double[11][HOURS];
		}
		samples[0][hour]++;
		if (flow >= 0) {
			samples[1][hour] += flow;
			samples[2][hour]++;
		}
		if (occupancy >= 0) {
			samples[3][hour] += occupancy;
			samples[4][hour]++;
		}
		if (speed > 0) {
			samples[7][hour] += speed;
			samples[8][hour]++;
			if (flow > 0) {
				samples[5][hour] += speed * flow;
				samples[6][hour] += flow;
			}
		}
		if (state > 0) {
			samples[9][hour] += state;
			samples[10][hour]++;
		}
	}

	/**
	 * Averages the samples of each hour that has at least minSamples of them; speed is flow-weighted when flow is known.
	 */
	public void finishSamples(int minSamples) {
		if (samples == null) {
			return;
		}
		for (int h = 0; h < HOURS; h++) {
			if (samples[0][h] < minSamples) {
				continue;
			}
			roadState[h] = 1;
			if (samples[2][h] > 0) {
				flow[h] = samples[1][h] / samples[2][h];
			}
			if (samples[4][h] > 0) {
				occupancy[h] = samples[3][h] / samples[4][h];
			}
			if (samples[8][h] > 0) {
				measuredSpeed[h] = samples[6][h] > 0 ? samples[5][h] / samples[6][h] : samples[7][h] / samples[8][h];
			}
			state[h] = samples[10][h] > 0 ? (int) Math.round(samples[9][h] / samples[10][h]) : stateFromOccupancy(occupancy[h]);
		}
		samples = null;
	}

	public static int stateFromOccupancy(double occupancy) {
		if (Double.isNaN(occupancy)) {
			return STATE_UNKNOWN;
		}
		return occupancy < 15 ? STATE_FLUID : occupancy < 30 ? STATE_PRE_SATURATED : occupancy < 50 ? STATE_SATURATED : STATE_BLOCKED;
	}

	static double[] nan() {
		double[] a = new double[HOURS];
		Arrays.fill(a, Double.NaN);
		return a;
	}
}
