package net.osmand.server.api.operation.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import net.osmand.server.api.operation.OperationContext;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;

/**
 * Reads users in batches (bounded memory). For a percent &lt; 100 it picks a representative stride sample
 * spread evenly across the whole id range, not just the first N users.
 */
class UserBatchReader {

	private static final int BATCH_SIZE = 1000;

	private final CloudUsersRepository usersRepository;

	UserBatchReader(CloudUsersRepository usersRepository) {
		this.usersRepository = usersRepository;
	}

	int total() {
		return (int) Math.min(usersRepository.count(), Integer.MAX_VALUE);
	}

	// percent of total, rounded up; null or >= 100 means all
	static int limit(int total, Integer percent) {
		if (percent == null || percent >= 100) {
			return total;
		}
		return (int) Math.ceil(total * Math.max(0, percent) / 100.0);
	}

	List<Integer> sample(int total, int userLimit, OperationContext ctx) {
		boolean sample = userLimit > 0 && userLimit < total;
		double step = sample ? (double) total / userLimit : 1.0;
		double nextPick = 0.0;
		long index = 0;
		List<Integer> ids = new ArrayList<>(Math.max(0, Math.min(userLimit, total)));
		int batch = 0;
		while (ids.size() < userLimit && !ctx.isCancelled()) {
			Page<CloudUser> users = usersRepository.findAll(PageRequest.of(batch++, BATCH_SIZE, Sort.by("id")));
			if (users.isEmpty()) {
				break;
			}
			for (CloudUser user : users) {
				if (ids.size() >= userLimit) {
					break;
				}
				if (!sample || index >= nextPick) {
					ids.add(user.id);
					nextPick += step;
				}
				index++;
			}
			if (!users.hasNext()) {
				break;
			}
		}
		return ids;
	}
}
