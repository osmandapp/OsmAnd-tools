package net.osmand.server.api.searchtest.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "run_result")
public class RunResult extends Result {

	@Column(name = "run_id", nullable = false)
	public Long runId;

	@Column(name = "gen_id", nullable = false)
	public Long genId;

	@Column(name = "min_distance")
	public Integer minDistance;

	@Column(name = "closest_result", length = 512)
	public String closestResult;

	@Column(name = "actual_place")
	public Integer actualPlace;

	@Column(name = "results_count")
	public Integer resultsCount;
}
