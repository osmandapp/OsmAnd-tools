package net.osmand.server.monitor;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import net.osmand.server.TelegramBotManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OsmAndServerMonitorTasks {

	private static final Log LOG = LogFactory.getLog(TelegramBotManager.class);
	
	private static final int SECOND = 1000;
	private static final int MINUTE = 60 * SECOND;
	private static final int HOUR = 60 * MINUTE;
	
	@Autowired
	TelegramBotManager telegram;

	LiveCheckInfo live = new LiveCheckInfo();
	BuildServerCheckInfo buildServer = new BuildServerCheckInfo();
	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	
    @Scheduled(fixedRate = 5 * MINUTE)
    public void checkOsmAndLiveStatus() {
    	try {
			URL url = new URL("http://osmand.net/api/osmlive_status");
			InputStream is = url.openConnection().getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String osmlivetime = br.readLine();
			format.setTimeZone(TimeZone.getTimeZone("UTC"));
			Date dt = format.parse(osmlivetime);
			br.close();
			long currentDelay = System.currentTimeMillis() - dt.getTime();
			if(currentDelay - live.previousOsmAndLiveDelay > 30 * MINUTE && currentDelay > HOUR) {
				telegram.sendMonitoringAlertMessage(getLiveDelayedMessage(currentDelay));
				live.previousOsmAndLiveDelay = currentDelay;
			}
			live.lastCheckTimestamp = System.currentTimeMillis();
			live.lastOsmAndLiveDelay = currentDelay;
		} catch (Exception e) {
			telegram.sendMonitoringAlertMessage("Exception while checking the server live status.");
			LOG.error(e.getMessage(), e);
		}
    }
    
    
    @Scheduled(fixedRate =  MINUTE)
    public void checkOsmAndBuildServer() {
    	try {
    		Set<String> jobsFailed = new TreeSet<String>();
			URL url = new URL("http://builder.osmand.net:8080/api/json");
			InputStream is = url.openConnection().getInputStream();
			JSONObject object = new JSONObject(new JSONTokener(is));
			JSONArray jsonArray = object.getJSONArray("jobs");
			for(int i = 0; i < jsonArray.length(); i++) {
				JSONObject jb = jsonArray.getJSONObject(i);
				String name = jb.getString("name");
				String color = jb.getString("color");
				if(!color.equals("blue") && !color.equals("disabled") && 
						!color.equals("notbuilt")) {
					jobsFailed.add(name);
				}
			}
			is.close();
			if(!buildServer.jobsFailed.equals(jobsFailed)) {
				Set<String> jobsFailedCopy = new TreeSet<String>(jobsFailed);
				jobsFailedCopy.removeAll(buildServer.jobsFailed);
				if(!jobsFailedCopy.isEmpty()) {
					telegram.sendMonitoringAlertMessage("There are new failures on OsmAnd Build Server: " + jobsFailedCopy);
				}
				
				Set<String> jobsRecoveredCopy = new TreeSet<String>(buildServer.jobsFailed);
				jobsRecoveredCopy.removeAll(jobsFailed);
				if(!jobsRecoveredCopy.isEmpty()) {
					telegram.sendMonitoringAlertMessage("There are recovered jobs on OsmAnd Build Server: " + jobsRecoveredCopy);
				}
				buildServer.jobsFailed = jobsFailed;
			}
			buildServer.lastCheckTimestamp = System.currentTimeMillis();
		} catch (Exception e) {
			telegram.sendMonitoringAlertMessage("Exception while checking the build server status.");
			LOG.error(e.getMessage(), e);
		}
    	
    }
    

    public String getStatusMessage() {
    	String msg = getLiveDelayedMessage(live.lastOsmAndLiveDelay) +"\n";
    	if(buildServer.jobsFailed.isEmpty()) {
    		msg += "OsmAnd Build server is OK.";
    	} else {
    		msg += "OsmAnd Build server has failing jobs: " + buildServer.jobsFailed;
    	}
    	return msg;
    }

	private String getLiveDelayedMessage(long delay) {
		int roundUp = (int) (delay * 100 / HOUR);
    	return "OsmAnd Live is delayed by " + (roundUp / 100.f) + " hours";
	}
	
	protected static class LiveCheckInfo {
		long previousOsmAndLiveDelay = 0;
		long lastOsmAndLiveDelay = 0;
		long lastCheckTimestamp = 0;
	}
	
	protected static class BuildServerCheckInfo {
		Set<String> jobsFailed = new TreeSet<String>(); 
		long lastCheckTimestamp = 0;
	}
	
}