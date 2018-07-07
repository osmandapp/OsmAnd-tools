package net.osmand.server.assist.ext;



import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.server.assist.OsmAndAssistantBot.MyDevicesOptions;
import net.osmand.server.assist.data.TrackerConfiguration;
import net.osmand.server.assist.data.UserChatIdentifier;

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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendLocation;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageLiveLocation;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.exceptions.TelegramApiException;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import com.google.gson.JsonObject;
import com.j256.simplecsv.common.CsvColumn;
import com.j256.simplecsv.processor.CsvProcessor;

@Component
public class MegaGPSTracker implements ITrackerManager {

	private static final String BASE_URL = "http://mega-gps.org/api3";
	public static final int SOCKET_TIMEOUT = 15 * 1000;
	public static final long INTERVAL_TO_UPDATE_GROUPS = 10000;
	public static final long INITIAL_TIMESTAMP_TO_DISPLAY = 120000;
	public static final Integer DEFAULT_LIVE_PERIOD = 24 * 60 * 60;
	
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(MegaGPSTracker.class);
	
	private Map<String, String> namesCache = new ConcurrentHashMap<String, String>();
	
	private Map<String, MonitorLocationGroup> monitorLocations = new ConcurrentHashMap<String, MonitorLocationGroup>();
	
	protected ThreadPoolExecutor exe;
	
	protected CloseableHttpClient httpclient;
	
	protected RequestConfig requestConfig;

	public MegaGPSTracker() {
		this.exe = new ThreadPoolExecutor(1, 10, 1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>());
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
			LOG.error(e.getMessage(), e);
			throw new IllegalArgumentException(e);
		}
	}
	
	
	@Scheduled(fixedRate = 5 * 1000)
    public void updateMonitoringDevices() {
		Iterator<Entry<String, MonitorLocationGroup>> it = monitorLocations.entrySet().iterator();
		long timestamp = System.currentTimeMillis();
		while(it.hasNext()) {
			MonitorLocationGroup locGroup = it.next().getValue();
			if(timestamp - locGroup.updateTime  > INTERVAL_TO_UPDATE_GROUPS) {
				TaskToUpdateLocationGroup task = new TaskToUpdateLocationGroup(locGroup);
				if(!exe.getQueue().contains(task)) {
					exe.submit(task);
				}
			}
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
	
	public void monitorDevices(OsmAndAssistantBot bt, UserChatIdentifier ucid, TrackerConfiguration c) {
		MonitorLocationGroup lg = new MonitorLocationGroup();
		lg.bot = bt;
		lg.config = c;
		lg.chatId = ucid.getChatId() + "";
		monitorLocations.put(lg.chatId, lg);
		exe.submit(new TaskToUpdateLocationGroup(lg));
	}
	
	public void stopMonitorDevices(OsmAndAssistantBot bt, UserChatIdentifier ucid, TrackerConfiguration c) {
		monitorLocations.remove(ucid.getChatId()+"");
	}

	public void retrieveInfoAboutMyDevice(OsmAndAssistantBot bot, UserChatIdentifier ucid, TrackerConfiguration c, 
			String dId, Message replyMessage) {
		exe.submit(new Runnable() {
			@Override
			public void run() {
				try {
					List<MegaGPSDevice> devs = getDevices(c, true, dId);
					if(devs.size() > 0) {
						MegaGPSDevice dv = devs.get(0);
						StringBuilder info = new StringBuilder();
						info.append("Device: ").append(dv.name == null ? dId : dv.name).append("\n");
						if(dv.timeLastReceived != 0) {
							info.append("Last signal received: \t").append(bot.formatTime(dv.timeLastReceived)).append("\n");
						}
						if(dv.timeLastValid != 0) {
							info.append("Last location received: \t").append(bot.formatTime(dv.timeLastValid)).append("\n");
						}
						if(dv.speed != 0 && dv.timeLastValid == dv.timeLastReceived) {
							info.append("Speed: \t").append(dv.speed).append("\n");
						}
						if(dv.temparature != -100) {
							info.append("Temparature: \t").append(dv.temparature).append("\n");
						}
						if(replyMessage != null) {
							if(!replyMessage.hasLocation()) {
								EditMessageText msg = new EditMessageText();
								msg.setChatId(ucid.getChatId());
								msg.setText(info.toString());
								msg.setMessageId(replyMessage.getMessageId());
								msg.setReplyMarkup(getKeyboardMarkup(c, dv));
								bot.sendMethod(msg);
							}
						} else {
							SendMessage msg = new SendMessage();
							msg.setChatId(ucid.getChatId());
							msg.setText(info.toString());
							msg.setReplyMarkup(getKeyboardMarkup(c, dv));
							bot.sendMethod(msg);
						}
						
						if (dv.getLatitude() != 0 || dv.getLongitude() != 0) {
							if(replyMessage != null) {
								if(replyMessage.hasLocation()) {
									EditMessageLiveLocation loc = new EditMessageLiveLocation();
									loc.setChatId(ucid.getChatId());
									loc.setMessageId(replyMessage.getMessageId());
									loc.setLatitude(dv.getLatitude());
									loc.setLongitud(dv.getLongitude());
									loc.setReplyMarkup(getKeyboardMarkup(c, dv));
									bot.sendMethod(loc);
								}
							} else {
								SendLocation loc = new SendLocation(dv.getLatitude(), dv.getLongitude());
								loc.setLivePeriod(DEFAULT_LIVE_PERIOD);
								loc.setChatId(ucid.getChatId());
								loc.setReplyMarkup(getKeyboardMarkup(c, dv));
								bot.sendMethod(loc);
							}
						}
						
					}
				} catch (Exception e) {
					processError(bot, ucid, e, "retrieving info about device");
				}
			}

			private InlineKeyboardMarkup getKeyboardMarkup(TrackerConfiguration c, MegaGPSDevice dv) {
				InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
				ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
				InlineKeyboardButton button = new InlineKeyboardButton("Update");
				button.setCallbackData("udevice|" + c.id + "|" + dv.id);
				lt.add(button);
				markup.getKeyboard().add(lt);
				return markup;
			}

			
		});		
	}
	
	
	private void processError(OsmAndAssistantBot bot, UserChatIdentifier ucid, Exception e, String action) {
		if (!(e instanceof TelegramApiException)) {
			try {
				bot.sendMethod(new SendMessage(ucid.getChatId(), "Error while " + action + ": " + e.getMessage()));
			} catch (TelegramApiException e1) {
			}
		}
		if (e instanceof TelegramApiException) {
			String emsg = ((TelegramApiException) e).getMessage();
			LOG.warn("Error while " + action + "\n" + emsg, e);
		} else {
			LOG.warn("Error while " + action, e);
		}
	}
	
	public void retrieveMyDevices(OsmAndAssistantBot bot, MyDevicesOptions options) {
		exe.submit(new Runnable() {
			@Override
			public void run() {
				try {
					List<MegaGPSDevice> devs = getDevices(options.config, options.filterLocationTime > 0, null);
					Iterator<MegaGPSDevice> it = devs.iterator();
					while(it.hasNext()) {
						MegaGPSDevice d = it.next();
						if(d.timeLastValid < options.filterLocationTime) {
							it.remove();
						}
					}
					SendMessage msg = new SendMessage();
					msg.setChatId(options.ucid.getChatId());
					printDevices(msg, devs, options, true);
					bot.sendMethod(msg);
				} catch (Exception e) {
					processError(bot, options.ucid, e, "retrieving my devices");
				}
			}

		});
	}


	protected String printDevices(SendMessage msg, List<MegaGPSDevice> devs, MyDevicesOptions opts, boolean keyboard) {
		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		StringBuilder s = new StringBuilder();
		if(opts.cfgOrder != 0) {
			s.append("Devices in '").append(opts.config.trackerName).append("':");
		} else {
			if(devs.size() == 0) {
				s.append("No available devices.");
			} else {
				s.append("Available devices:");
			}
		}
		s.append('\n');
		int k = 1;
		for(MegaGPSDevice d : devs) {
			StringBuilder txt = new StringBuilder(40); 
			txt.append(opts.cfgOrder == 0 ? "" : (opts.cfgOrder +".")).append(k).append(k < 10 ? " " : "").append("  ");
			if(d.name != null) {
				txt.append(d.name);
			}
			if(d.id != null) {
				txt.append(" (").append(d.id).append(")");
			}
//			if(k < 3) {
//				s.append(d.toString());
//			}
			if(!keyboard) {
				s.append(txt).append('\n');
			} else {
				ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
				InlineKeyboardButton button = new InlineKeyboardButton(txt.toString());
				button.setCallbackData("device|" + opts.config.id + "|" + d.id);
				lt.add(button);
				markup.getKeyboard().add(lt);
			}
			k++;
		}
		if(keyboard) {
			msg.setReplyMarkup(markup);
		}
		msg.setText(s.toString());
		return s.toString();
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

	public boolean accept(TrackerConfiguration c) {
		return c.trackerId.equals("http://mega-gps.org/") || c.trackerId.equals("mega-gps.org");
	}

	public class TaskToUpdateLocationGroup implements Runnable {

		private MonitorLocationGroup group;
		
		public TaskToUpdateLocationGroup(MonitorLocationGroup group) {
			this.group = group;
		}
		
		@Override
		public void run() {
			try {
				long time = System.currentTimeMillis();
				if(group.initialTimestamp == 0) {
					group.initialTimestamp = time;
				} 
				group.updateTime = time;
				List<MegaGPSDevice> allDevices = getDevices(group.config, true, null);
				for (MegaGPSDevice dd : allDevices) {
					if (dd.timeLastValid * 1000 < time - INITIAL_TIMESTAMP_TO_DISPLAY) {
						continue;
					}
					final MessageToDevice existingMessage = getMessageToDevice(time, dd);
					if (existingMessage.device != null && existingMessage.device.lat == dd.lat
							&& existingMessage.device.lng == dd.lng 
							&& existingMessage.device.azi == dd.azi
							// creates extra traffic for updates
							&& existingMessage.device.timeLastReceived == dd.timeLastReceived  
							) {
						continue;
					}
					existingMessage.updateId++;
					existingMessage.device = dd;
					existingMessage.updateTime = time;
					String bld = buildMessage(existingMessage);
					if (existingMessage.messageId == null) {
						SendMessage msg = new SendMessage();
						msg.setChatId(group.chatId);
						msg.setText(bld);
						group.bot.sendMethodAsync(msg, new SentCallback<Message>() {

							@Override
							public void onResult(BotApiMethod<Message> method, Message response) {
								existingMessage.messageId = response.getMessageId();
							}

							@Override
							public void onError(BotApiMethod<Message> method, TelegramApiRequestException apiException) {
							}

							@Override
							public void onException(BotApiMethod<Message> method, Exception exception) {
							}
						});
					} else {
						EditMessageText msg = new EditMessageText();
						msg.setChatId(group.chatId);
						msg.setText(bld);
						msg.setMessageId(existingMessage.messageId);
						group.bot.sendMethod(msg);

					}
					
				}
			} catch (Exception e) {
				LOG.error(e.getMessage() ,e);
			}
		}

		private MessageToDevice getMessageToDevice(long time, MegaGPSDevice dd) {
			MessageToDevice existingMessage = group.group.get(dd.id);
			if(existingMessage == null) {
				existingMessage = new MessageToDevice();
				existingMessage.initialTimestamp = time;
				group.group.put(dd.id, existingMessage);
			}
			return existingMessage;
		}
		
		public String buildMessage(MessageToDevice existingMessage) {
			MegaGPSDevice dd = existingMessage.device;
			long time = existingMessage.updateTime;
			JsonObject obj = new JsonObject();
			obj.addProperty("name", dd.name == null ? dd.id : dd.name);
			obj.addProperty("lat", (Float)dd.getLatitude());
			obj.addProperty("lon", (Float)dd.getLongitude());
			if(dd.altitude != 0) {
				obj.addProperty("alt", (Float)dd.altitude);
			}
			if(dd.azi != 0 && dd.timeLastReceived == dd.timeLastValid) {
				obj.addProperty("azi", (Float)dd.azi);
			}
			if(dd.speed != 0 && dd.timeLastReceived == dd.timeLastValid) {
				obj.addProperty("spd", (Float)dd.speed);
			}
			if(dd.timeLastReceived != 0) {
				obj.addProperty("updAgo", (Long)(time/ 1000 - dd.timeLastReceived));
			}
			if(dd.timeLastValid != 0) {
				obj.addProperty("locAgo", (Long)(time/ 1000 - dd.timeLastValid));
			}
			obj.addProperty("updId", (Integer)existingMessage.updateId);
			obj.addProperty("updTime", (Long)((existingMessage.updateTime - existingMessage.initialTimestamp)/1000));
			return obj.toString();
		}


		@Override
		public boolean equals(Object obj) {
			if (this == obj || (obj instanceof TaskToUpdateLocationGroup && 
					((TaskToUpdateLocationGroup)obj).group == group)) {
				return true;
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return group == null ? 0 : group.hashCode();
		}

	}



public static class MonitorLocationGroup {
		
		public OsmAndAssistantBot bot;
		
		public String chatId;
		
		public long initialTimestamp;
		
		public long updateTime;
		
		public TrackerConfiguration config;
		
		public ConcurrentHashMap<String, MessageToDevice> group = new ConcurrentHashMap<String, MessageToDevice>();
		
	}
	
	public static class MessageToDevice {
		
		public long initialTimestamp;
		
		public long updateTime;
		
		public Integer messageId ;
		
		public boolean isLiveLocation;
		
		public String deviceId;
		
		public MegaGPSDevice device;

		public int updateId ;
		
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
		float speed;
		
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
