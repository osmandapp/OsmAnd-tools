package net.osmand.server.api.services;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Remembers the last routes each signed-in user asked this server for, so the manual check can take the points of
 * a drive someone moved on the map in the frame. Locally the frame is the web map on :3000, which is another origin:
 * the page cannot read its address, but the map asks /routing/route through its proxy to this very server, with
 * the session cookie of the user who opened the page - a cookie is not bound to a port.
 *
 * Only the points are kept, a few per user, and nothing at all for anyone not signed in.
 */
@Component
public class TurnLanesRouteCapture implements Filter {

	private static final String PATH = "/routing/route";
	private static final int KEPT = 10;

	public static class Captured {
		/** "lat,lon" each: the start, the points it goes through, the end */
		public List<String> points;
		public String routeMode;
		public long time;
	}

	private final Map<String, Deque<Captured>> byUser = new ConcurrentHashMap<>();

	@Override
	public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
			throws IOException, ServletException {
		try {
			if (req instanceof HttpServletRequest http && PATH.equals(http.getRequestURI())) {
				capture(http);
			}
		} catch (RuntimeException e) {
			// never in the way of the route itself
		}
		chain.doFilter(req, resp);
	}

	private void capture(HttpServletRequest http) {
		String[] points = http.getParameterValues("points");
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (points == null || points.length < 2 || auth == null || !auth.isAuthenticated()
				|| "anonymousUser".equals(auth.getName())) {
			return;
		}
		Captured c = new Captured();
		c.points = new ArrayList<>(Arrays.asList(points));
		c.routeMode = http.getParameter("routeMode");
		c.time = System.currentTimeMillis();
		Deque<Captured> q = byUser.computeIfAbsent(auth.getName(), k -> new ArrayDeque<>());
		synchronized (q) {
			q.addFirst(c);
			while (q.size() > KEPT) {
				q.removeLast();
			}
		}
	}

	/** the user's routes, the latest first */
	public List<Captured> recent(String user) {
		Deque<Captured> q = byUser.get(user);
		if (q == null) {
			return List.of();
		}
		synchronized (q) {
			return new ArrayList<>(q);
		}
	}
}
