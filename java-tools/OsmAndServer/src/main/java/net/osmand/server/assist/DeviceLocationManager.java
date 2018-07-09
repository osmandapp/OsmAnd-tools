package net.osmand.server.assist;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import net.osmand.server.assist.data.Device;
import net.osmand.server.assist.data.DeviceRepository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.exceptions.TelegramApiException;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import com.google.gson.JsonObject;

@Component
public class DeviceLocationManager {
	
	@Autowired
	OsmAndAssistantBot assistantBot;
	
	@Autowired
	DeviceRepository deviceRepo;
	
	ConcurrentHashMap<Long, DeviceMonitor> devices = new ConcurrentHashMap<>();

	private static final Log LOG = LogFactory.getLog(DeviceLocationManager.class);
	

	public String sendLocation(String deviceId, String lat, String lon)  throws DeviceNotFoundException {
		long did = Device.getDecodedId(deviceId);
		if(!deviceRepo.existsById(did)) {
            throw new DeviceNotFoundException(); 
		}
		DeviceMonitor dm = devices.get(did);
		if (dm != null) {
			double lt = Double.NaN;
			double ln = Double.NaN;
			if (lat != null && lon != null) {
				lt = Double.parseDouble(lat);
				ln = Double.parseDouble(lon);
			}
			dm.setLocation(lt, ln);
			for (LocationMessage lm : dm.chats.values()) {
				if(lm.isEnabled()) {
					lm.sendMessage(assistantBot);
				}
			}
		}
		return "OK";
	}
	
	@ResponseStatus(value = HttpStatus.NOT_FOUND)
	public static class DeviceNotFoundException extends RuntimeException {

		private static final long serialVersionUID = 8429349261883069752L;

		
	}

	public boolean isLocationMonitored(Device d, Long chatId) {
		DeviceMonitor dm = devices.get(d.id);
		if(dm != null) {
			LocationMessage lm = dm.chats.get(chatId);
			if(lm != null) {
				return lm.isEnabled();
			}
		}
		return false;
	}
	

	public void startMonitoringLocation(Device d, Long chatId) throws TelegramApiException {
		DeviceMonitor dm = devices.get(d.id);
		if(dm == null) {
			dm = new DeviceMonitor(d);
			devices.put(d.id, dm);
		}
		LocationMessage lm = dm.chats.get(chatId);
		if(lm == null) {
			lm = new LocationMessage(dm, chatId);
			dm.chats.put(chatId, lm);
		}
		lm.enable();
		lm.sendMessage(assistantBot);
	}

	public void stopMonitoringLocation(Device d, Long chatId) {
		DeviceMonitor dm = devices.get(d.id);
		if(dm != null) {
			LocationMessage lm = dm.chats.get(chatId);
			if(lm != null) {
				lm.disable();
			}
		}		
	}
	

	
	protected static class DeviceMonitor {
		ConcurrentHashMap<Long, LocationMessage> chats = new ConcurrentHashMap<>();
		final Device device;
		
		double lat = Double.NaN;
		double lon = Double.NaN;
		double speed = 0;
		double altitude = Double.NaN;
		double azi = Double.NaN;
		long locationTimestamp = 0;
		boolean locationLost = false;
		
		protected DeviceMonitor(Device device) {
			this.device = device;
		}

		public void setLocation(double lt, double ln) {
			if(Double.isNaN(lt)) {
				locationLost = true;
			} else {
				locationLost = false;
				locationTimestamp = System.currentTimeMillis();
				lat = lt;
				lon = ln;
				speed = 0;
				azi = Double.NaN;
				altitude = Double.NaN;
			}
		}
	}
	
	protected static class LocationMessage {
		
		final long chatId;
		final DeviceMonitor mon;
		
		int messageId;
		long initialTimestamp = System.currentTimeMillis();
		long updateTime;
		boolean isLiveLocation;
		
		int updateId = 1;
		private boolean enabled;
		private long disabledTimestamp;
		
		
		public LocationMessage(DeviceMonitor d, Long chatId) {
			mon = d;
			this.chatId = chatId;
		}
		
		public void enable() {
			this.enabled = true;
		}
		
		public boolean isEnabled() {
			return enabled;
		}
		
		public void disable() {
			disabledTimestamp = System.currentTimeMillis();
			enabled = false;
		}


		public String sendMessage(OsmAndAssistantBot bot) {
			JsonObject obj = new JsonObject();
			updateTime = System.currentTimeMillis();
			obj.addProperty("name", mon.device.deviceName);
			obj.addProperty("id", mon.device.getEncodedId());
			if(!Double.isNaN(mon.lat)) {
				obj.addProperty("lat", (float)mon.lat);
			}
			if(!Double.isNaN(mon.lon)) {
				obj.addProperty("lon", (float)mon.lon);
			}
			if(!Double.isNaN(mon.altitude) && !mon.locationLost) {
				obj.addProperty("alt", (float)mon.altitude);
			}
			if(!Double.isNaN(mon.azi) && !mon.locationLost) {
				obj.addProperty("azi", (float)mon.azi);
			}
			if(mon.speed != 0 && !mon.locationLost) {
				obj.addProperty("spd", (float)mon.speed);
			}
			if(mon.locationTimestamp != 0) {
				obj.addProperty("locAgo", (Long)(updateTime/ 1000 - mon.locationTimestamp));
			}
//			if(dd.timeLastValid != 0) {
//				obj.addProperty("locAgo", (Long)(time/ 1000 - dd.timeLastValid));
//			}
			obj.addProperty("updId", updateId++);
			obj.addProperty("updTime", (Long)((updateTime - initialTimestamp)/1000));
			if(messageId == 0) {
				bot.sendMethodAsync(new SendMessage(chatId, obj.toString()), new SentCallback<Message>() {
					
					@Override
					public void onResult(BotApiMethod<Message> method, Message response) {
						messageId = response.getMessageId();
					}
					
					@Override
					public void onException(BotApiMethod<Message> method, Exception exception) {
						LOG.error(exception.getMessage(), exception);
					}
					
					@Override
					public void onError(BotApiMethod<Message> method, TelegramApiRequestException apiException) {
						LOG.error(apiException.getMessage(), apiException);
					}
				});
			} else {
				EditMessageText mtd = new EditMessageText();
				mtd.setChatId(chatId);
				mtd.setMessageId(messageId);
				mtd.setText(obj.toString());
				bot.sendMethodAsync(mtd, new SentCallback<Serializable>() {
					@Override
					public void onResult(BotApiMethod<Serializable> method, Serializable response) {
					}
					
					@Override
					public void onException(BotApiMethod<Serializable> method, Exception exception) {
						LOG.error(exception.getMessage(), exception);
					}
					
					@Override
					public void onError(BotApiMethod<Serializable> method, TelegramApiRequestException apiException) {
						LOG.error(apiException.getMessage(), apiException);
					}
				});
			}
			return obj.toString();
		}
	}
}
