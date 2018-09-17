package net.osmand.server.assist.ext;



import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import net.osmand.server.assist.data.DeviceMonitor;
import net.osmand.server.assist.data.LocationInfo;
import net.osmand.server.assist.data.TrackerConfiguration;

import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import com.j256.simplecsv.common.CsvColumn;
import com.j256.simplecsv.processor.CsvProcessor;

@Component
public class MegaGPSTracker implements ITrackerManager {

	private static final String BASE_URL = "http://mega-gps.org/api3";
	public static final int SOCKET_TIMEOUT = 15 * 1000;
	
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(MegaGPSTracker.class);
	
	private Map<String, String> namesCache = new ConcurrentHashMap<String, String>();
	protected CloseableHttpClient httpclient;
	protected RequestConfig requestConfig;
	
	public boolean accept(TrackerConfiguration c) {
		return c.trackerId.equals("http://mega-gps.org/") || c.trackerId.equals("mega-gps.org");
	}

	public MegaGPSTracker() {
        httpclient = HttpClientBuilder.create()
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setConnectionTimeToLive(SOCKET_TIMEOUT, TimeUnit.MILLISECONDS)
                .setMaxConnTotal(20)
                .build();
        requestConfig = RequestConfig.copy(RequestConfig.custom().build())
                .setSocketTimeout(SOCKET_TIMEOUT)
                .setConnectTimeout(SOCKET_TIMEOUT)
                .setConnectionRequestTimeout(SOCKET_TIMEOUT).build();
	}
	
	public String getTrackerId() {
		return "mega-gps.org";
	}
	
	@Override
	public void init(TrackerConfiguration ext) {
	}


	@Override
	public List<? extends DeviceInfo> getDevicesList(TrackerConfiguration config) {
		try {
			return getDevices(config, false, null);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	
	
	@Override
	public void updateDeviceMonitors(TrackerConfiguration ext, Map<String, List<DeviceMonitor>> mp) {
		try {
			List<MegaGPSDevice> allDevices = getDevices(ext, true, null);
			long time = System.currentTimeMillis();
			for (MegaGPSDevice dd : allDevices) {
				if (dd.timeLastValid < time / 1000  - 90 * 24 * 60 * 60) {
					continue;
				}
				List<DeviceMonitor> list = mp.get(dd.id);
				if (list != null) {
					LocationInfo li = new LocationInfo(dd.timeLastReceived * 1000);
					LocationInfo location = new LocationInfo(dd.timeLastValid * 1000);
					// Assume location is valid in case 
					// 1. last location received no later than 15 seconds from signal 
					// 2. less than an hour from now
					if(dd.timeLastValid < dd.timeLastReceived - 15 && 
							System.currentTimeMillis() / 1000 - dd.timeLastValid < 60 * 60) {
						li = location;
					}
					location.setLatLon(dd.getLatitude(), dd.getLongitude());
					location.setAltitude(dd.altitude);
					location.setAzi(dd.azi);
					if(dd.speed >= 0) {
						location.setSpeed(dd.speed);
					}
					if(dd.satellites != 0) {
						location.setSatellites(dd.satellites);
					}
					if(dd.temparature != -100) {
						li.setTemperature(dd.temparature);
					}
					// li.setIpSender(ipSender);
					for (DeviceMonitor dm : list) {
						LocationInfo lastSignal = dm.getLastSignal();
						if (lastSignal != null && li.getTimestamp() == lastSignal.getTimestamp()) {
							continue;
						}
						dm.sendLocation(li, location);
					}
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
	
	
	

	private List<MegaGPSDevice> getDevices(TrackerConfiguration c, String apiMethod, String deviceId) throws IOException, ClientProtocolException {
		String url = BASE_URL;
		HttpPost httppost = new HttpPost(url);
		httppost.setConfig(requestConfig);
		httppost.addHeader("charset", StandardCharsets.UTF_8.name());

		List<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("s", c.token));
		params.add(new BasicNameValuePair("c", apiMethod));
		params.add(new BasicNameValuePair("i", deviceId));

		httppost.setEntity(new UrlEncodedFormEntity(params));
		List<MegaGPSDevice> devs = Collections.emptyList();
		try (CloseableHttpResponse response = httpclient.execute(httppost)) {
			HttpEntity ht = response.getEntity();
			BufferedHttpEntity buf = new BufferedHttpEntity(ht);
			String result = EntityUtils.toString(buf, StandardCharsets.UTF_8);
			devs = parseDevices(result);
		}
		return devs;
	}
	
	private List<MegaGPSDevice> getDevices(TrackerConfiguration c, boolean withLocations, String trackerId) throws IOException, ClientProtocolException {
		if(!withLocations) {
			List<MegaGPSDevice> devices = getDevices(c, "0", trackerId == null ?  "0" : trackerId);
			updateCacheNames(devices);
			return devices;
		}
		List<MegaGPSDevice> devices = getDevices(c, "1", trackerId == null ?  "0" : trackerId);
		for(MegaGPSDevice d : devices) {
			if(!namesCache.containsKey(d.id)) {
				updateCacheNames(getDevices(c, "0", "0"));
				break;
			}
		}
		for(MegaGPSDevice d : devices) {
			d.name = namesCache.get(d.id);
		}
		return devices;
	}
	
	private void updateCacheNames(List<MegaGPSDevice> devices) {
		for(MegaGPSDevice d : devices) {
			if(d.id != null && d.name != null) {
				namesCache.put(d.id, d.name);
			}
		}
		
	}
	
	
	protected List<MegaGPSDevice> parseDevices(String result) {
		List<MegaGPSDevice> devices = new ArrayList<MegaGPSTracker.MegaGPSDevice>();
		CsvProcessor<MegaGPSDevice> csvProcessor = new CsvProcessor<MegaGPSDevice>(MegaGPSDevice.class);
		csvProcessor.setFlexibleOrder(true);
		csvProcessor.setColumnSeparator(';');
		csvProcessor.setAllowPartialLines(true);
		csvProcessor.setHeaderValidation(true);
		csvProcessor.setIgnoreUnknownColumns(true);
		try {
			devices = csvProcessor.readAll(new StringReader(result), null);
		} catch (ParseException e) {
			LOG.warn(e.getMessage(), e);
		} catch (IOException e) {
			LOG.warn(e.getMessage(), e);
		}
		return devices;
	}


	public static class MegaGPSDevice implements DeviceInfo {
		@CsvColumn(columnName="id")
		String id;
		@CsvColumn(columnName="name", mustBeSupplied=false)
		String name;
		
		@CsvColumn(columnName="extra", mustBeSupplied=false)
		String extra;
		
		@CsvColumn(columnName="tlast", mustBeSupplied=false)
		long timeLastReceived;
		
		@CsvColumn(columnName="tvalid", mustBeSupplied=false)
		long timeLastValid;
		
		@CsvColumn(columnName="tarc", mustBeSupplied=false)
		long timeLastInContinuousArchive;
		
		@CsvColumn(columnName="lat", mustBeSupplied=false)
		int lat;
		
		@CsvColumn(columnName="lng", mustBeSupplied=false)
		int lng;
		
		@CsvColumn(columnName="speed", mustBeSupplied=false)
		float speed = -100;
		
		@CsvColumn(columnName="alt", mustBeSupplied=false)
		float altitude;
		
		@CsvColumn(columnName="azi", mustBeSupplied=false)
		float azi;
		
		@CsvColumn(columnName="sat", mustBeSupplied=false)
		int satellites;
		
		@CsvColumn(columnName="temp", mustBeSupplied=false)
		int temparature = -100;
		
		public float getLongitude() {
			return lng / 1000000.0f;
		}
		
		public float getLatitude() {
			return lat / 1000000.0f;
		}

		@Override
		public String toString() {
			return "MegaGPSDevice (id=" + id + ", name=" + name + ", extra=" + extra + ", timeLastReceived="
					+ timeLastReceived + ", timeLastValid=" + timeLastValid + ", timeLastInContinuousArchive="
					+ timeLastInContinuousArchive + ", lat=" + lat + ", lng=" + lng + ", speed=" + speed
					+ ", altitude=" + altitude + ", azi=" + azi + ", sattelites=" + satellites + ", temparature="
					+ temparature + ")";
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public String getId() {
			return id;
		}
		
		
	}
	
	
}

	
