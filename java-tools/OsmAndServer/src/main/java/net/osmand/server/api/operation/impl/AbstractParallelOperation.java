package net.osmand.server.api.operation.impl;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

import net.osmand.server.api.operation.Operation;
import net.osmand.server.api.operation.OperationContext;

public abstract class AbstractParallelOperation<P> implements Operation<P> {

	protected static final int MAX_THREADS = 10;

	protected static int clampThreads(Integer threads) {
		return threads == null ? 1 : Math.max(1, Math.min(MAX_THREADS, threads));
	}

	protected <T> void forEach(int threads, List<T> items, OperationContext ctx, Consumer<T> action) {
		if (threads <= 1) {
			for (T item : items) {
				if (ctx.isCancelled()) {
					break;
				}
				action.accept(item);
			}
			return;
		}
		ForkJoinPool pool = new ForkJoinPool(threads);
		try {
			pool.submit(() -> items.parallelStream().forEach(item -> {
				if (!ctx.isCancelled()) {
					action.accept(item);
				}
			})).get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			throw new IllegalStateException(e.getCause());
		} finally {
			pool.shutdown();
		}
	}
}
