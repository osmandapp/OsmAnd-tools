package net.osmand.server.assist.data;

import com.google.gson.JsonObject;
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

import javax.persistence.*;
import java.io.Serializable;
import java.util.*;

public class Device {

	private final OsmAndAssistantBot bot;
	private DeviceBean deviceBean;
	private final DeviceRepository deviceRepository;


	private enum ChatType {
		MESSAGE_CHAT,
		JSON_CHAT,
		MAP_CHAT,
		MESSAGE_INLINE,
		MAP_INLINE,
	}

	private Set<LocationChatMessage> chats;


	// signal with last location
	LocationInfo lastSignal = new LocationInfo();
	// signal with last location
	LocationInfo lastLocationSignal = null;

	private boolean enabled;
	private long disabledTimestamp;

	public Device(DeviceBean deviceBean, OsmAndAssistantBot bot, DeviceRepository deviceRepository) {
		this.deviceBean = deviceBean;
		this.bot = bot;
		this.deviceRepository = deviceRepository;
		this.chats = new HashSet<>(deviceBean.chatMessages);
	}


	public String getExternalId() {
		return deviceBean.externalId;
	}

	public String getDeviceName() {
		return deviceBean.deviceName;
	}

	public String getStringId() {
		return deviceBean.getEncodedId();
	}

	public Date getCreatedDate() {
		return deviceBean.createdDate;
	}


	public TrackerConfiguration getExternalConfiguration() {
		return deviceBean.externalConfiguration;
	}

	public JsonObject getMessageJson(int updateId) {
		JsonObject obj = new JsonObject();
		LocationInfo lastSignal = this.lastSignal;
		LocationInfo lastLocSig = this.lastLocationSignal;
		obj.addProperty("name", deviceBean.deviceName);
		obj.addProperty("id", deviceBean.getEncodedId());
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
		bld.append(String.format("<b>Device</b>: %s\n<b>Location</b>: %s\n", deviceBean.deviceName, locMsg));
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
		Set<LocationChatMessage> ch = this.chats;
		for(LocationChatMessage m : ch) {
			if(m.isEnabled(now)) {
				m.sendMessage(this);
			}

		}
	}

	public void startMonitoring() {
		this.enabled = true;
	}

	private void addChatMessage(LocationChatMessage lm) {
		Set<LocationChatMessage>  n = new HashSet<>(chats);
		n.add(lm);
		setNewChats(n);
	}


	private void setNewChats(Set<LocationChatMessage> n) {
		long now = System.currentTimeMillis();
		Iterator<LocationChatMessage> it = n.iterator();
		while(it.hasNext()) {
			LocationChatMessage msg = it.next();
			if(!msg.isEnabled(now)) {
				it.remove();
			}
		}
		this.chats = n;
	}

	private LocationChatMessage getOrCreate(ChatType tp, Long chatId) {
		for(LocationChatMessage lm : chats) {
			if(lm.chatId == chatId && lm.type == tp) {
				return lm;
			}
		}
		LocationChatMessage lm = new LocationChatMessage(tp, chatId);
		addChatMessage(lm);
		return lm;
	}

	private LocationChatMessage getOrCreate(ChatType tp, String inlineMsgId) {
		for(LocationChatMessage lm : chats) {
			if(Algorithms.objectEquals(inlineMsgId, lm.inlineMessageId) && lm.type == tp) {
				return lm;
			}
		}
		LocationChatMessage lm = new LocationChatMessage(tp, inlineMsgId);
		addChatMessage(lm);
		return lm;
	}

	public void showLiveMap(Long chatId) {
		getOrCreate(ChatType.MAP_CHAT, chatId).sendMessage(this);
	}

	public void showLiveMessage(Long chatId) {
		getOrCreate(ChatType.MESSAGE_CHAT, chatId).sendMessage(this);
	}

	public void showLiveMessage(String inlineMsgId) {
		getOrCreate(ChatType.MESSAGE_INLINE, inlineMsgId).sendMessage(this);
	}

	public void showLiveMap(String inlineMsgId) {
		getOrCreate(ChatType.MAP_INLINE, inlineMsgId).sendMessage(this);
	}

	public void hideMessage(Long chatId, int messageId) {
		for(LocationChatMessage m : chats) {
			if(m.messageId == messageId && m.chatId == chatId) {
				m.hide();
			}
		}
		// refresh list to delete hidden
		setNewChats(new HashSet<>(chats));
	}

	public void stopMonitoring() {
		disable();
	}


	public DeviceBean getDevice() {
		return deviceBean;
	}

	public long getOwnerId() {
		return deviceBean.userId;
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


	@Entity(name = "LocationChatMessage")
	@Table(name = "telegram_chat_messages")
	public static class LocationChatMessage {

		private static final Integer DEFAULT_UPD_PERIOD = 86400;
		private static final Log LOG = LogFactory.getLog(LocationChatMessage.class);

		protected static final int ERROR_THRESHOLD = 3;

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public long id;

		@Column(name = "chat_type", nullable = false)
		public ChatType type;

		@Column(name = "chat_id", nullable = false)
		public long chatId;

		@Column(name = "inline_message_id")
		public String inlineMessageId;

		@Column(name = "initial_timestamp", nullable = false)
		public long initialTimestamp;

		@Column(name = "hidden")
		public boolean hidden;

		@Column(name = "message_id")
		public int messageId;

		@Column(name = "update_time")
		public long updateTime;

		@Column(name = "update_id")
		public int updateId = 1;

		@Column(name = "error_count")
		public int errorCount;

		@OneToOne(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
		@JoinColumn(name = "location_info_id")
		LocationInfo lastSentLoc;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			LocationChatMessage that = (LocationChatMessage) o;
			return chatId == that.chatId &&
					initialTimestamp == that.initialTimestamp &&
					type == that.type;
		}

		@Override
		public int hashCode() {
			return Objects.hash(type, chatId, initialTimestamp);
		}

		public LocationChatMessage() { }

		public LocationChatMessage(ChatType type, long chatId) {
			this.type = type;
			this.chatId = chatId;
			this.inlineMessageId = null;
			this.initialTimestamp = System.currentTimeMillis();
		}

		public LocationChatMessage(ChatType type, String inlineMessageId) {
			this.type = type;
			this.chatId = 0;
			this.inlineMessageId = inlineMessageId;
			this.initialTimestamp = System.currentTimeMillis();
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

		public int deleteOldMessage(Device device) {
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


		public void sendMessage(Device device) {
			LocationInfo lastSignal= device.lastLocationSignal;
			updateTime = System.currentTimeMillis();
			if(!isEnabled(updateTime)) {
				messageId = 0;
			}
			if(type == ChatType.MAP_CHAT) {
				sendMapMessage(device.bot, lastSignal, device);
			} else if(type == ChatType.JSON_CHAT) {
				sendMsg(device.bot, device.getMessageJson(updateId).toString(), lastSignal, device);
			} else if(type == ChatType.MESSAGE_CHAT) {
				sendMsg(device.bot, device.getMessageTxt(updateId), lastSignal, device);
			} else if(type == ChatType.MAP_INLINE) {
				sendInlineMap(device.bot, lastSignal, device);
			} else if(type == ChatType.MESSAGE_INLINE) {
				sendInline(device.bot, lastSignal, device);
			}
			updateId++;
		}

		private void sendInlineMap(OsmAndAssistantBot bot, LocationInfo locSig, Device device) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData(
					"msg|" + device.getStringId() + "|updmap")));
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

					bot.sendMethodAsync(editMessageText, getCallback(locSig, device));
				}
			}
		}

		private void sendInline(OsmAndAssistantBot bot, LocationInfo lastLocationSignal, Device device) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData(
					"msg|" + device.getStringId() + "|updtxt")));
			EditMessageText editMessageText = new EditMessageText();
			editMessageText.setText(device.getMessageTxt(updateId));
			editMessageText.enableHtml(true);
			editMessageText.setReplyMarkup(markup);
			editMessageText.setInlineMessageId(inlineMessageId);
			bot.sendMethodAsync(editMessageText, getCallback(lastLocationSignal, device));
		}

		private void sendMapMessage(OsmAndAssistantBot bot, LocationInfo locSig, Device device) {
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
					bot.sendMethodAsync(sl, getCallback(locSig, device));
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
						bot.sendMethodAsync(sl, getCallback(locSig, device));
					}
				}
			}
		}

		private void sendMsg(OsmAndAssistantBot bot, String text, LocationInfo lastLocSignal, Device device) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Hide").setCallbackData(
					"dv|" + device.getStringId() + "|hide")));
			if (messageId == 0) {
				bot.sendMethodAsync(new SendMessage(chatId, text).setReplyMarkup(markup).enableHtml(true),
						getCallback(lastLocSignal, device));
			} else {
				EditMessageText mtd = new EditMessageText();
				mtd.setChatId(chatId);
				mtd.setMessageId(messageId);
				mtd.setText(text);
				mtd.enableHtml(true);
				mtd.setReplyMarkup(markup);
				bot.sendMethodAsync(mtd, getCallback(lastLocSignal, device));
			}
		}

		private <T extends Serializable> SentCallback<T> getCallback(LocationInfo locSig, Device device) {
			return new SentCallback<T>() {
				@Override
				public void onResult(BotApiMethod<T> method, T response) {
					long locationId = 0;
					if (lastSentLoc != null) {
						locationId = lastSentLoc.id;
					}
					lastSentLoc = locSig;
					lastSentLoc.id = locationId;
					if(response instanceof Message) {
						DeviceBean bean = device.deviceBean;
						messageId = ((Message)response).getMessageId();
						bean.chatMessages.addAll(device.chats);
						bean = device.deviceRepository.save(bean);
						device.deviceBean = bean;
						device.chats = bean.chatMessages;
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
						hidden = true;
					}
				}
			};
		}
	}
}