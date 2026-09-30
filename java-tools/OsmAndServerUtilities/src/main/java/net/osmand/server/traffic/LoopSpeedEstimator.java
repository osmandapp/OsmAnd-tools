package net.osmand.server.traffic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Speed from a single induction loop: speed = flow * g / occupancy.
 * <p>
 * g is the effective length (vehicle + loop, and in practice also the number of lanes that are summed into one flow
 * but averaged into one occupancy). It is calibrated per sensor on light-traffic hours under the assumption that
 * traffic then moves at the free-flow speed (the road's max speed from the map) - the PeMS "g-factor" method.
 * Checked on Madrid detectors that also measure speed: median error 10.9 km/h in slow hours, against 42.6 km/h for a
 * constant free-flow speed.
 */
public class LoopSpeedEstimator {

	public double minFreeFlow = 60; // veh/h, fewer vehicles make occupancy too coarse
	public double minFreeOccupancy = 1;
	public double maxFreeOccupancy = 8;
	public int minFreeHours = 3;
	public double minOccupancy = 0.5;
	public double minSpeed = 3;
	public double maxSpeedFactor = 1.3;

	/**
	 * @return effective length in km, NaN when the sensor has too few light-traffic hours
	 */
	public double calibrate(TrafficSensor sensor, double freeFlowKmh) {
		List<Double> lengths = new ArrayList<>();
		for (int h = 0; h < TrafficSensor.HOURS; h++) {
			double q = sensor.flow[h], k = sensor.occupancy[h];
			if (q >= minFreeFlow && k >= minFreeOccupancy && k <= maxFreeOccupancy) {
				lengths.add(freeFlowKmh * k / 100 / q);
			}
		}
		return lengths.size() >= minFreeHours ? median(lengths) : Double.NaN;
	}

	public double estimate(double flow, double occupancy, double effectiveLength, double freeFlowKmh) {
		if (!(flow > 0) || !(occupancy >= minOccupancy) || Double.isNaN(effectiveLength)) {
			return Double.NaN;
		}
		double v = flow * effectiveLength / (occupancy / 100);
		return Math.max(minSpeed, Math.min(freeFlowKmh * maxSpeedFactor, v));
	}

	public void estimate(TrafficSensor sensor) {
		for (int h = 0; h < TrafficSensor.HOURS; h++) {
			sensor.estimatedSpeed[h] = sensor.isFaulty(h) ? Double.NaN
					: estimate(sensor.flow[h], sensor.occupancy[h], sensor.effectiveLength, sensor.freeFlowSpeed);
		}
	}

	public static double median(List<Double> values) {
		if (values.isEmpty()) {
			return Double.NaN;
		}
		List<Double> sorted = new ArrayList<>(values);
		Collections.sort(sorted);
		int n = sorted.size();
		return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
	}
}
