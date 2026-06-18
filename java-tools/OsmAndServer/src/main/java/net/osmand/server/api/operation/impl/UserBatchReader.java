package net.osmand.server.api.operation.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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

	void forEachBatch(int total, int userLimit, OperationContext ctx, Consumer<List<Integer>> batchConsumer) {
		boolean sample = userLimit > 0 && userLimit < total;
		double step = sample ? (double) total / userLimit : 1.0;
		double nextPick = 0.0;
		long index = 0;
		int selected = 0;
		int batch = 0;
		while (selected < userLimit && !ctx.isCancelled()) {
			Page<CloudUser> users = usersRepository.findAll(PageRequest.of(batch++, BATCH_SIZE, Sort.by("id")));
			if (users.isEmpty()) {
				break;
			}
			List<Integer> ids = new ArrayList<>();
			for (CloudUser user : users) {
				if (selected >= userLimit) {
					break;
				}
				if (!sample || index >= nextPick) {
					ids.add(user.id);
					selected++;
					nextPick += step;
				}
				index++;
			}
			if (!ids.isEmpty()) {
				batchConsumer.accept(ids);
			}
			if (!users.hasNext()) {
				break;
			}
		}
	}
}
