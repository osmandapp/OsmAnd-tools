package net.osmand.server.controllers.pub;

import java.io.File;
import java.io.Serial;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jakarta.servlet.annotation.WebListener;
import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpSessionEvent;
import jakarta.servlet.http.HttpSessionListener;

import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.primitives.Metadata;
import org.springframework.stereotype.Component;

import net.osmand.router.RouteCalculationProgress;

@WebListener
@Component
public class UserSessionResources implements HttpSessionListener {

	private static final Log LOG = LogFactory.getLog(UserSessionResources.class);

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

	public void addGpxTempFilesToSession(HttpSession httpSession, File gpxFile) {
		if (httpSession == null || gpxFile == null) {
			return;
		}
		GPXSessionContext ctx = getGpxResources(httpSession);
		ctx.tempFiles.add(gpxFile);
		ctx.tempFiles.add(new File(gpxFile.getAbsolutePath() + ".gz"));
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
		if (ctx != null && ctx.tempFiles != null) {
			for (File f : ctx.tempFiles) {
				if (f == null) {
					continue;
				}
				try {
					Files.deleteIfExists(f.toPath());
				} catch (Exception e) {
					LOG.warn("Session cleanup: failed to delete temp file: " + f.getAbsolutePath(), e);
					f.deleteOnExit();
				}
			}
			ctx.tempFiles.clear();
			ctx.files.clear();
		}
	}
}
