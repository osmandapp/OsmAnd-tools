package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.primitives.Metadata;
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
		GpxTrackAnalysis analysis;
		Metadata metadata;
		GpxTrackAnalysis srtmAnalysis;
	}
	
	public static class GPXSessionContext implements Serializable {
		@Serial
		private static final long serialVersionUID = 1L;
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
