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

@WebListener
@Component
public class UserSessionResources implements HttpSessionListener {

	protected static final String SESSION_GPX = "gpx";
	
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
