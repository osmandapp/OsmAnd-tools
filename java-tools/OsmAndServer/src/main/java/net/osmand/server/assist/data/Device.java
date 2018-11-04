package net.osmand.server.assist.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import net.osmand.server.assist.DeviceLocationManager;
import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendLocation;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageLiveLocation;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Device {
	private static final Log LOG = LogFactory.getLog(DeviceLocationManager.class);
	private static final Integer DEFAULT_UPD_PERIOD = 86400;
	
	final OsmAndAssistantBot bot;
	final DeviceBean device;
	
	private enum ChatType {
		MESSAGE_CHAT,
		JSON_CHAT,
		MAP_CHAT,
		MESSAGE_INLINE,
		MAP_INLINE,
	}
	
	List<LocationChatMessage> chats = new ArrayList<Device.LocationChatMessage>();
	
	
	// signal with last location 
	LocationInfo lastSignal = new LocationInfo();
	// signal with last location 
	LocationInfo lastLocationSignal = null;
	
	private boolean enabled;
	private long disabledTimestamp;

	public Device(DeviceBean device, OsmAndAssistantBot bot) {
		this.device = device;
		this.bot = bot;
		
		if(device.data.has(DeviceBean.CHATS_INFO)) {
			loadChatsFromCache(device.data.get(DeviceBean.CHATS_INFO).getAsJsonArray());
		}
	}
	
	
	private void loadChatsFromCache(JsonArray ar) {
		for (JsonElement e : ar) {
			try {
				LocationChatMessage msg = new LocationChatMessage(this, e.getAsJsonObject());
				this.chats.add(msg);
			} catch (RuntimeException es) {
				LOG.warn("Error parsing " + e.toString(), es);
			}
		}
	}


	public String getExternalId() {
		return device.externalId;
	}
	
	public String getDeviceName() {
		return device.deviceName;
	}
	
	public String getStringId() {
		return device.getEncodedId();
	}
	
	public Date getCreatedDate() {
		return device.createdDate;
	}
	
	
	public TrackerConfiguration getExternalConfiguration() {
		return device.externalConfiguration;
	}
	
	public JsonObject getMessageJson(int updateId) {
		JsonObject obj = new JsonObject();
		LocationInfo lastSignal = this.lastSignal;
		LocationInfo lastLocSig = this.lastLocationSignal;
		obj.addProperty("name", device.deviceName);
		obj.addProperty("id", device.getEncodedId());
		boolean locationCurrentlyPresent = lastSignal.isLocationPresent();
		// display location from lastSignal anyway though it could be deprecated
		if (locationCurrentlyPresent) {
			obj.addProperty("lat", (float) lastSignal.lat);
			obj.addProperty("lon", (float) lastSignal.lon);
			obj.addProperty("locTime", (Long) (lastSignal.timestamp) / 1000);
		} else if (lastLocSig != null && lastLocSig.isLocationPresent()) {
			obj.addProperty("lat", (float) lastLocSig.lat);
			obj.addProperty("lon", (float) lastLocSig.lon);
			obj.addProperty("locTime", (Long) (lastLocSig.timestamp) / 1000);
		}
		if (!Double.isNaN(lastSignal.altitude) && locationCurrentlyPresent) {
			obj.addProperty("alt", (float) lastSignal.altitude);
		}
		
		if (!Double.isNaN(lastSignal.azi) && locationCurrentlyPresent) {
			obj.addProperty("azi", (float) lastSignal.azi);
		}
		if (!Double.isNaN(lastSignal.speed) && locationCurrentlyPresent) {
			obj.addProperty("spd", (float) lastSignal.speed);
		}
		if (!Double.isNaN(lastSignal.satellites) && locationCurrentlyPresent) {
			obj.addProperty("sat", (int) lastSignal.satellites);
		}
		if (!Double.isNaN(lastSignal.hdop) && locationCurrentlyPresent) {
			obj.addProperty("hdop", (int) lastSignal.hdop);
		}
		if (!Double.isNaN(lastSignal.temperature)) {
			obj.addProperty("temp", (float) lastSignal.temperature);
		}
		obj.addProperty("updTime", (Long) (lastSignal.timestamp / 1000));
		obj.addProperty("updId", updateId++);
		return obj;
	}
	
	public String getMessageTxt(int updateId) {
		LocationInfo lastSignal = this.lastSignal;
		LocationInfo lastLocSig = this.lastLocationSignal;
		StringBuilder bld = new StringBuilder();
		String locMsg = bot.formatLocation(lastSignal, true);
		bld.append(String.format("<b>Device</b>: %s\n<b>Location</b>: %s\n", device.deviceName, locMsg));
		if(!lastSignal.isLocationPresent() && lastLocSig != null && lastLocSig.isLocationPresent()) {
			bld.append(String.format("<b>Last location</b>: %s\n", bot.formatLocation(lastLocSig, false)));
		}
		boolean locationCurrentlyPresent = lastSignal.isLocationPresent();
		if (!Double.isNaN(lastSignal.altitude) && lastSignal.isLocationPresent()) {
			bld.append(String.format("<b>Altitude</b>: %.1f\n", (float) lastSignal.altitude));
		}
		if (!Double.isNaN(lastSignal.azi) && locationCurrentlyPresent) {
			bld.append(String.format("<b>Bearing</b>: %.1f\n", (float) lastSignal.azi));
		}
		if (!Double.isNaN(lastSignal.speed) && locationCurrentlyPresent) {
			bld.append(String.format("<b>Speed</b>: %.1f\n", lastSignal.speed));
		}
		if (!Double.isNaN(lastSignal.satellites) && locationCurrentlyPresent) {
			bld.append(String.format("<b>Sattelites</b>: %d\n", (int) (int) lastSignal.satellites));
		}
		if (!Double.isNaN(lastSignal.hdop) && locationCurrentlyPresent) {
			bld.append(String.format("<b>Horizontal precision</b>: %d\n", (int) lastSignal.hdop));
		}
		if (!Double.isNaN(lastSignal.temperature)) {
			bld.append(String.format("<b>Temperature</b>: %.1f\n", lastSignal.temperature));
		}
		if(updateId == 0) {
			bld.append(String.format("Updated: %s\n", bot.formatFullTime(lastSignal.getTimestamp())));
		} else {
			bld.append(String.format("Updated: %s (%d)\n", bot.formatFullTime(lastSignal.getTimestamp()) ,updateId));
		}
		return bld.toString().trim();
	
	}
	
	public void sendLocation(LocationInfo info) {
		LocationInfo locSignal = lastLocationSignal;
		if(info.isLocationPresent()) {
			locSignal = info;
		}
		sendLocation(info, locSignal);
	}

	public void sendLocation(LocationInfo info, LocationInfo locSignal) {
		lastSignal = info;
		lastLocationSignal = locSignal;
		long now = System.currentTimeMillis();
		List<LocationChatMessage> ch = this.chats;
		for(LocationChatMessage m : ch) {
			if(m.isEnabled(now)) {
				m.sendMessage();
			}
			
		}
	}
	
	public void startMonitoring() {
		this.enabled = true;
	}
	
	private void addChatMessage(LocationChatMessage lm) {
		List<LocationChatMessage>  n = new ArrayList<>(chats);
		n.add(lm);
		setNewChats(n);
	}
	
	private void removeHiddenChats() {
		List<LocationChatMessage> n = new ArrayList<>(chats);
		setNewChats(n);
	}


	private void setNewChats(List<LocationChatMessage> n) {
		long now = System.currentTimeMillis();
		Iterator<LocationChatMessage> it = n.iterator();
		while(it.hasNext()) {
			LocationChatMessage msg = it.next();
			if(!msg.isEnabled(now)) {
				it.remove();
			}
		}
		this.chats = n;
		JsonArray ar = new JsonArray();
		for(LocationChatMessage l : n) {
			ar.add(l.getAsJsonObject());
		}
		device.data.add(DeviceBean.CHATS_INFO, ar);
		bot.saveDeviceInfo(device);
	}
	
	private LocationChatMessage getOrCreate(ChatType tp, Long chatId) {
		for(LocationChatMessage lm : chats) {
			if(lm.chatId == chatId.longValue() && lm.type == tp) {
				return lm;
			}
		}
		LocationChatMessage lm = new LocationChatMessage(this, tp, chatId);
		addChatMessage(lm);
		return lm;
	}
	
	private LocationChatMessage getOrCreate(ChatType tp, String inlineMsgId) {
		for(LocationChatMessage lm : chats) {
			if(Algorithms.objectEquals(inlineMsgId, lm.inlineMessageId) && lm.type == tp) {
				return lm;
			}
		}
		LocationChatMessage lm = new LocationChatMessage(this, tp, inlineMsgId);
		addChatMessage(lm);
		return lm;
	}
	
	public void showLiveMap(Long chatId) {
		getOrCreate(ChatType.MAP_CHAT, chatId).sendMessage();
	}
	
	public void showLiveMessage(Long chatId) {
		getOrCreate(ChatType.MESSAGE_CHAT, chatId).sendMessage();
	}
	
	public void showLiveMessage(String inlineMsgId) {
		getOrCreate(ChatType.MESSAGE_INLINE, inlineMsgId).sendMessage();
	}
	
	public void showLiveMap(String inlineMsgId) {
		getOrCreate(ChatType.MAP_INLINE, inlineMsgId).sendMessage();
	}
	
	public void hideInlineMsg(String inlineMsgId) {
		boolean hd = false;
		for(LocationChatMessage lm : chats) {
			if(Algorithms.objectEquals(inlineMsgId, lm.inlineMessageId) ) {
				if(lm.type == ChatType.MAP_INLINE) {
					lm.sendInlineMap(bot, null, true);
				} else if(lm.type == ChatType.MESSAGE_INLINE) {
					lm.sendInline(bot, null, true);
				}
				lm.hide();
				hd = true;
			}
		}
		if(hd) {
			removeHiddenChats();
		}
	}
	
	
	public void hideMessage(Long chatId, int messageId) {
		for(LocationChatMessage m : chats) {
			if(m.messageId == messageId && m.chatId == chatId.longValue()) {
				m.hide();
			}
		}
		// refresh list to delete hidden
		setNewChats(new ArrayList<>(chats));
	}
	
	public void stopMonitoring() {
		disable();
	}
	
	
	public DeviceBean getDevice() {
		return device;
	}
	
	public long getOwnerId() {
		return device.userId;
	}
	
	public LocationInfo getLastSignal() {
		return lastSignal;
	}
	
	public LocationInfo getLastLocationSignal() {
		return lastLocationSignal;
	}
	
	
	public long getDisabledTimestamp() {
		return disabledTimestamp;
	}

	public void disable() {
		disabledTimestamp = System.currentTimeMillis();
		enabled = false;
	}
	
	public boolean isLocationMonitored() {
		return enabled;
	}
	

	public class LocationChatMessage {
		protected static final int ERROR_THRESHOLD = 3;
		
		protected static final String CHAT_TYPE = "chat_type";
		protected static final String CHAT_ID = "chat_id";
		protected static final String INLINE_MSG_ID = "inline_msg_id";
		protected static final String MSG_ID = "msg_id";
		protected static final String INITIAL_TIMESTAMP = "initial_timestamp";

		final Device device;
		final ChatType type;
		final long chatId;
		final String inlineMessageId;
		final long initialTimestamp;
		
		boolean hidden = false;
		int messageId;
		LocationInfo lastSentLoc;
		long updateTime;
		int updateId = 1;
		int errorCount = 0;
		
		public LocationChatMessage(Device d, JsonObject e) {
			device = d;
			this.type = ChatType.valueOf(e.get(CHAT_TYPE).getAsString());
			this.chatId = Long.parseLong(e.get(CHAT_ID).getAsString());
			this.inlineMessageId = e.has(INLINE_MSG_ID) ?  e.get(INLINE_MSG_ID).getAsString() : null;
			this.initialTimestamp = Long.parseLong(e.get(INITIAL_TIMESTAMP).getAsString());
			if(e.has(MSG_ID)) {
				this.messageId = Integer.parseInt(e.get(MSG_ID).getAsString());
			}
		}
		
		public LocationChatMessage(Device d, ChatType type, long chatId) {
			device = d;
			this.type = type;
			this.chatId = chatId;
			this.inlineMessageId = null;
			this.initialTimestamp = System.currentTimeMillis();
		}
		
		public LocationChatMessage(Device d, ChatType type, String inlineMessageId) {
			device = d;
			this.type = type;
			this.chatId = 0;
			this.inlineMessageId = inlineMessageId;
			this.initialTimestamp = System.currentTimeMillis();
		}
		
		public JsonObject getAsJsonObject() {
			JsonObject json = new JsonObject();
			json.addProperty(CHAT_TYPE, type.name());
			json.addProperty(CHAT_ID, chatId +"");
			json.addProperty(INITIAL_TIMESTAMP, initialTimestamp +"");
			if(inlineMessageId != null) {
				json.addProperty(INLINE_MSG_ID, inlineMessageId +"");
			}
			if(messageId != 0) {
				json.addProperty(MSG_ID, messageId +"");
			}
			return json;
		}
		
		public void hide() {
			hidden = true;
		}


		public boolean isEnabled(long now) {
			return !hidden &&  (now - initialTimestamp) < DEFAULT_UPD_PERIOD * 1000 ;
		}

		public boolean deleteMessage(int msgId) {
			if(this.messageId == msgId) {
				messageId = 0;
				return true;
			}
			return false;
		}

		public int deleteOldMessage() {
			int oldMessageId = messageId;
			if(messageId != 0) {
				device.bot.sendMethodAsync(new DeleteMessage(chatId, messageId), new SentCallback<Boolean>() {
					@Override
					public void onResult(BotApiMethod<Boolean> method, Boolean response) {
						
					}

					@Override
					public void onException(BotApiMethod<Boolean> method, Exception exception) {
						LOG.error(exception.getMessage(), exception);
					}

					@Override
					public void onError(BotApiMethod<Boolean> method, TelegramApiRequestException apiException) {
						LOG.error(apiException.getMessage(), apiException);
					}
				});
				messageId = 0;
			}
			return oldMessageId;
		}
		
		
		public void sendMessage() {
			LocationInfo lastSignal= device.lastLocationSignal;
			updateTime = System.currentTimeMillis();
			if(!isEnabled(updateTime)) {
				messageId = 0;
			}
			if(type == ChatType.MAP_CHAT) {
				sendMapMessage(bot, lastSignal);
			} else if(type == ChatType.JSON_CHAT) {
				sendMsg(bot, device.getMessageJson(updateId).toString(), lastSignal);
			} else if(type == ChatType.MAP_CHAT) {
				sendMsg(bot, device.getMessageTxt(updateId), lastSignal);
			} else if(type == ChatType.MAP_INLINE) {
				sendInlineMap(bot, lastSignal, false);
			} else if(type == ChatType.MESSAGE_INLINE) {
				sendInline(bot, lastSignal, false);
			}
			updateId++;
		}

		private void sendInlineMap(OsmAndAssistantBot bot, LocationInfo locSig, boolean hide) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			List<InlineKeyboardButton> lst = new ArrayList<>();
			if(hide) {
				lst.add(new InlineKeyboardButton("Start update " + device.getDeviceName()).setCallbackData(
						"msg|" + device.getStringId() + "|startmap"));
			} else {
				lst.add(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData(
					"msg|" + device.getStringId() + "|updmap"));
				lst.add(new InlineKeyboardButton("Stop").setCallbackData(
					"msg|" + device.getStringId() + "|hide"));
			}
			markup.getKeyboard().add(lst);
			if (locSig != null && locSig.isLocationPresent()) {
				if (lastSentLoc == null
						|| MapUtils.getDistance(lastSentLoc.getLat(), lastSentLoc.getLon(), locSig.getLat(),
								locSig.getLon()) > 5) {
					EditMessageLiveLocation editMessageText = new EditMessageLiveLocation();
					editMessageText.setInlineMessageId(inlineMessageId);
					editMessageText.setChatId("");
					editMessageText.setLatitude((float) locSig.getLat());
					editMessageText.setLongitud((float) locSig.getLon());
					editMessageText.setChatId((String)null);
					editMessageText.setReplyMarkup(markup);
					
					bot.sendMethodAsync(editMessageText, getCallback(locSig));
				}
			}
		}
		
		private void sendInline(OsmAndAssistantBot bot, LocationInfo lastLocationSignal, boolean hide) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			List<InlineKeyboardButton> lst = new ArrayList<>();
			if(hide) {
				lst.add(new InlineKeyboardButton("Start update " + device.getDeviceName()).setCallbackData(
						"msg|" + device.getStringId() + "|starttxt"));
			} else {
				lst.add(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData(
						"msg|" + device.getStringId() + "|updtxt"));
					lst.add(new InlineKeyboardButton("Stop").setCallbackData(
						"msg|" + device.getStringId() + "|hide"));
			}
			markup.getKeyboard().add(lst);
			EditMessageText editMessageText = new EditMessageText();
			editMessageText.setText(getMessageTxt(updateId));
			editMessageText.enableHtml(true);
			editMessageText.setReplyMarkup(markup);
			editMessageText.setInlineMessageId(inlineMessageId);
			bot.sendMethodAsync(editMessageText, getCallback(lastLocationSignal));
		}
		
		private void sendMapMessage(OsmAndAssistantBot bot, LocationInfo locSig) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
			markup.getKeyboard().add(lt);
			lt.add(new InlineKeyboardButton("Hide").setCallbackData("dv|" + device.getStringId() + "|hide"));
			lt.add(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData("dv|" + device.getStringId() + "|loc"));
			if (locSig != null && locSig.isLocationPresent()) {
				if (messageId == 0) {
					SendLocation sl = new SendLocation((float) locSig.getLat(), (float) locSig.getLon());
					sl.setChatId(chatId);
					sl.setLivePeriod(DEFAULT_UPD_PERIOD);
					sl.setReplyMarkup(markup);
					bot.sendMethodAsync(sl, getCallback(locSig));
				} else {
					if (lastSentLoc != null
							&& MapUtils.getDistance(lastSentLoc.getLat(), lastSentLoc.getLon(), locSig.getLat(),
									locSig.getLon()) > 5) {
						EditMessageLiveLocation sl = new EditMessageLiveLocation();
						sl.setMessageId(messageId);
						sl.setChatId(chatId);
						sl.setLatitude((float) locSig.getLat());
						sl.setLongitud((float) locSig.getLon());
						sl.setReplyMarkup(markup);
						bot.sendMethodAsync(sl, getCallback(locSig));
					}
				}
			}
		}

		private void sendMsg(OsmAndAssistantBot bot, String text, LocationInfo lastLocSignal) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Hide").setCallbackData(
					"dv|" + device.getStringId() + "|hide")));
			if (messageId == 0) {
				bot.sendMethodAsync(new SendMessage(chatId, text).setReplyMarkup(markup).enableHtml(true), 
						getCallback(lastLocSignal));
			} else {
				EditMessageText mtd = new EditMessageText();
				mtd.setChatId(chatId);
				mtd.setMessageId(messageId);
				mtd.setText(text);
				mtd.enableHtml(true);
				mtd.setReplyMarkup(markup);
				bot.sendMethodAsync(mtd, getCallback(lastLocSignal));
			}
		}
		
		private <T extends Serializable> SentCallback<T> getCallback(LocationInfo locSig) {
			return new SentCallback<T>() {
				@Override
				public void onResult(BotApiMethod<T> method, T response) {
					lastSentLoc = locSig;
					if(response instanceof Message) {
						messageId = ((Message)response).getMessageId();
					}
				}

				@Override
				public void onException(BotApiMethod<T> method, Exception exception) {
					LOG.error(exception.getMessage(), exception);
				}

				@Override
				public void onError(BotApiMethod<T> method,
						TelegramApiRequestException apiException) {
					LOG.info(apiException.getMessage(), apiException);
					// message expired or deleted
					if(errorCount++ > ERROR_THRESHOLD) {
//						errorCount = 0;
						hidden = true;
					}
				}
			};
		}
	}


	
}