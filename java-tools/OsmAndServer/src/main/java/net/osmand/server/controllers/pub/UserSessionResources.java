package net.osmand.server.controllers.pub;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.springframework.stereotype.Component;

import net.osmand.gpx.GPXTrackAnalysis;
import net.osmand.gpx.GPXUtilities;
import net.osmand.router.RouteCalculationProgress;

@WebListener
@Component
public class UserSessionResources implements HttpSessionListener {

	protected static final String SESSION_GPX = "gpx";
	protected static final String SESSION_ROUTING = "routing";
	
	static class GPXSessionFile {
		transient File file;
		double size;
		GPXTrackAnalysis analysis;
		GPXUtilities.Metadata metadata;
		GPXTrackAnalysis srtmAnalysis;
	}
	
	public static class GPXSessionContext {
		List<GPXSessionFile> files = new ArrayList<>();
		public List<File> tempFiles = new ArrayList<>();
	}
	
	public GPXSessionContext getGpxResources(HttpSession httpSession) {
		GPXSessionContext ctx = (GPXSessionContext) httpSession.getAttribute(SESSION_GPX);
		if (ctx == null) {
			ctx = new GPXSessionContext();
			httpSession.setAttribute(SESSION_GPX, ctx);
		}
		return ctx;
	}
	
	public RouteCalculationProgress getRoutingProgress(HttpSession session) {
		if (session.getAttribute(SESSION_ROUTING) instanceof RouteCalculationProgress) {
			System.out.printf("DEBUG: cancel previous progress, last isCancelled (%d)\n",
					((RouteCalculationProgress) session.getAttribute(SESSION_ROUTING)).isCancelled == true ? 1 : 0);
			System.out.printf("DEBUG: previous progress: %s\n",
					((RouteCalculationProgress) session.getAttribute(SESSION_ROUTING)).getInfo(null));
			((RouteCalculationProgress) session.getAttribute(SESSION_ROUTING)).isCancelled = true;
		}
		RouteCalculationProgress progress = new RouteCalculationProgress();
		session.setAttribute(SESSION_ROUTING, progress);
		return progress;
	}
	
	@Override
	public void sessionCreated(HttpSessionEvent se) {
	}

	@Override
	public void sessionDestroyed(HttpSessionEvent se) {
		GPXSessionContext ctx = (GPXSessionContext) se.getSession().getAttribute(SESSION_GPX);
		if (ctx != null) {
            for (File f : ctx.tempFiles) {
				f.delete();
			}
			ctx.tempFiles.clear();
			ctx.files.clear();
		}
	}
}
