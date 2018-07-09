package net.osmand.server.assist.data;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import net.osmand.server.assist.DeviceLocationManager;
import net.osmand.server.assist.OsmAndAssistantBot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import com.google.gson.JsonObject;

public class DeviceMonitor {
	private static final Log LOG = LogFactory.getLog(DeviceLocationManager.class);
	ConcurrentHashMap<Long, LocationChatMessage> chats = new ConcurrentHashMap<>();
	final OsmAndAssistantBot bot;
	final Device device;

	double lat = Double.NaN;
	double lon = Double.NaN;
	double speed = 0;
	double altitude = Double.NaN;
	double azi = Double.NaN;
	long locationTimestamp = 0;
	boolean locationLost = false;

	public DeviceMonitor(Device device, OsmAndAssistantBot bot) {
		this.device = device;
		this.bot = bot;
	}

	public void setLocation(double lt, double ln) {
		if (Double.isNaN(lt)) {
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
	
	public void notifyChats() {
		for (LocationChatMessage lm : chats.values()) {
			if (lm.isEnabled()) {
				lm.sendMessage();
			}
		}
	}
	
	public LocationChatMessage getOrCreateLocationChat(Long chatId) {
		LocationChatMessage lm = chats.get(chatId);
		if(lm == null) {
			lm = new LocationChatMessage(this, chatId);
			chats.put(chatId, lm);
		}
		return lm;
	}
	
	public LocationChatMessage getLocationChat(Long chatId) {
		return chats.get(chatId);
	}
	
	public Device getDevice() {
		return device;
	}
	
	public boolean isLocationMonitored(Long chatId) {
		LocationChatMessage lm = chats.get(chatId);
		if(lm != null) {
			return lm.isEnabled();
		}
		return false;
	}
	
	public boolean isOneChatMonitored() {
		for(LocationChatMessage m : chats.values()) {
			if(m.isEnabled()) {
				return true; 
			}
		}
		return false;
	}

	public static class LocationChatMessage {

		final long chatId;
		final DeviceMonitor mon;

		int messageId;
		long initialTimestamp = System.currentTimeMillis();
		long updateTime;
		boolean isLiveLocation;

		int updateId = 1;
		private boolean enabled;
		private long disabledTimestamp;

		public LocationChatMessage(DeviceMonitor d, Long chatId) {
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

		public String sendMessage() {
			JsonObject obj = new JsonObject();
			updateTime = System.currentTimeMillis();
			obj.addProperty("name", mon.device.deviceName);
			obj.addProperty("id", mon.device.getEncodedId());
			if (!Double.isNaN(mon.lat)) {
				obj.addProperty("lat", (float) mon.lat);
			}
			if (!Double.isNaN(mon.lon)) {
				obj.addProperty("lon", (float) mon.lon);
			}
			if (!Double.isNaN(mon.altitude) && !mon.locationLost) {
				obj.addProperty("alt", (float) mon.altitude);
			}
			if (!Double.isNaN(mon.azi) && !mon.locationLost) {
				obj.addProperty("azi", (float) mon.azi);
			}
			if (mon.speed != 0 && !mon.locationLost) {
				obj.addProperty("spd", (float) mon.speed);
			}
			if (mon.locationTimestamp != 0) {
				obj.addProperty("locAgo", (Long) (updateTime / 1000 - mon.locationTimestamp));
			}
			// if(dd.timeLastValid != 0) {
			// obj.addProperty("locAgo", (Long)(time/ 1000 - dd.timeLastValid));
			// }
			obj.addProperty("updId", updateId++);
			obj.addProperty("updTime", (Long) ((updateTime - initialTimestamp) / 1000));
			if (messageId == 0) {
				mon.bot.sendMethodAsync(new SendMessage(chatId, obj.toString()), new SentCallback<Message>() {

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
				mon.bot.sendMethodAsync(mtd, new SentCallback<Serializable>() {
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