package net.osmand.server.assist.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import net.osmand.server.assist.DeviceLocationManager;
import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendLocation;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageLiveLocation;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.Location;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import com.google.gson.JsonObject;

public class DeviceMonitor {
	private static final Log LOG = LogFactory.getLog(DeviceLocationManager.class);
	private static final Integer DEFAULT_UPD_PERIOD = 86400;
	
	ConcurrentHashMap<Long, LocationChatMessage> chats = new ConcurrentHashMap<>();
	ConcurrentHashMap<Long, LocationChatMessage> mapChats = new ConcurrentHashMap<>();
	
	final OsmAndAssistantBot bot;
	final Device device;
	// signal with last location 
	LocationInfo lastSignal = new LocationInfo();
	// signal with last location 
	LocationInfo lastLocationSignal = null;
	
	private boolean enabled;
	private long disabledTimestamp;

	public DeviceMonitor(Device device, OsmAndAssistantBot bot) {
		this.device = device;
		this.bot = bot;
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
		for (LocationChatMessage lm : chats.values()) {
			if (lm.isEnabled(now)) {
				lm.sendMessage(bot, device, info, locSignal);
			}
		}
		for (LocationChatMessage lm : mapChats.values()) {
			if (lm.isEnabled(now)) {
				lm.sendMessage(bot, device, info, locSignal);
			}
		}
	}
	
	public void startMonitoring() {
		this.enabled = true;
		
	}
	
	public void showLiveMessage(Long chatId) {
		LocationChatMessage lm = getOrCreateLocationChat(chatId);
		lm.sendMessage(bot, device, lastSignal, lastLocationSignal);
	}
	
	public void showLiveMap(Long chatId) {
		LocationChatMessage lm = getOrCreateLocationMapChat(chatId);
		lm.sendMessage(bot, device, lastSignal, lastLocationSignal);
	}
	
	
	public void hideMessage(Long chatId, int messageId) {
		LocationChatMessage lm = chats.get(chatId);
		if(lm != null) {
			lm.deleteMessage(messageId);
		}
		lm = mapChats.get(chatId);
		if(lm != null) {
			lm.deleteMessage(messageId);
		}
	}
	public void stopMonitoring() {
		disable();
	}
	
	public LocationChatMessage getOrCreateLocationMapChat(Long chatId) {
		LocationChatMessage lm = chats.get(chatId);
		if(lm == null) {
			lm = new LocationChatMessage(this, chatId);
			lm.isLiveLocation = true;
			mapChats.put(chatId, lm);
		}
		return lm;
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
	

	public static class LocationChatMessage {
		private boolean JSON_MESSAGE = false;
		
		final long chatId;
		final DeviceMonitor mon;

		int messageId;
		long initialTimestamp;
		long updateTime;
		
		boolean isLiveLocation;
		LocationInfo lastSentLoc;

		int updateId = 1;
		

		public LocationChatMessage(DeviceMonitor d, Long chatId) {
			mon = d;
			this.chatId = chatId;
		}


		public boolean isEnabled(long now) {
			return messageId != 0 && (now - initialTimestamp) < DEFAULT_UPD_PERIOD * 1000 ;
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
				mon.bot.sendMethodAsync(new DeleteMessage(chatId, messageId), new SentCallback<Boolean>() {
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
		
		public String getMessageTxt(OsmAndAssistantBot bot, Device device, 
				LocationInfo lastSignal, LocationInfo lastSig) {
			
			StringBuilder bld = new StringBuilder();
			String locMsg = bot.formatLocation(lastSignal);
			bld.append(String.format("<b>Device</b>: %s\n<b>Location</b>: %s\n", device.deviceName, locMsg));
			if(!lastSignal.isLocationPresent() && lastSig != null && lastSig.isLocationPresent()) {
				bld.append(String.format("<b>Last location</b>: %s\n", bot.formatLocation(lastSig)));
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
			bld.append(String.format("Updated: %s (%d)\n", bot.formatFullTime(lastSignal.getTimestamp()) ,updateId++));
			return bld.toString().trim();
		
		}
		public void sendMessage(OsmAndAssistantBot bot, Device device, 
				LocationInfo lastSignal, LocationInfo lastLocationSignal) {
			updateTime = System.currentTimeMillis();
			if(!isEnabled(updateTime)) {
				messageId = 0;
			}
			if(isLiveLocation) {
				sendMapMessage(bot, device, lastSignal, lastLocationSignal);
			} else {
				String txt = JSON_MESSAGE ? getMessageJson(bot, device, lastSignal, lastLocationSignal).toString()
						: getMessageTxt(bot, device, lastSignal, lastLocationSignal);
				sendMsg(bot, device, txt);
			}
		}
		
		private void sendMapMessage(OsmAndAssistantBot bot, Device d, LocationInfo lastSignal,
				LocationInfo locSig) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
			markup.getKeyboard().add(lt);
			lt.add(new InlineKeyboardButton("Hide").setCallbackData("hide"));
			lt.add(new InlineKeyboardButton("Update " + d.deviceName).setCallbackData("dv|" + d.getEncodedId() + "|loc"));
			if (locSig != null && locSig.isLocationPresent()) {
				if (messageId == 0) {
					initialTimestamp = System.currentTimeMillis();
					SendLocation sl = new SendLocation((float) locSig.getLat(), (float) locSig.getLon());
					sl.setChatId(chatId);
					sl.setLivePeriod(DEFAULT_UPD_PERIOD);
					sl.setReplyMarkup(markup);
					bot.sendMethodAsync(sl, new SentCallback<Message>() {

						@Override
						public void onResult(BotApiMethod<Message> method, Message response) {
							messageId = response.getMessageId();
							lastSentLoc = locSig;
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
					if (lastSentLoc != null
							&& MapUtils.getDistance(lastSentLoc.getLat(), lastSentLoc.getLon(), locSig.getLat(),
									locSig.getLon()) > 5) {
						EditMessageLiveLocation sl = new EditMessageLiveLocation();
						sl.setMessageId(messageId);
						sl.setChatId(chatId);
						sl.setLatitude((float) locSig.getLat());
						sl.setLongitud((float) locSig.getLon());
						sl.setReplyMarkup(markup);
						bot.sendMethodAsync(sl, new SentCallback<Serializable>() {
							@Override
							public void onResult(BotApiMethod<Serializable> method, Serializable response) {
								lastSentLoc = locSig;
							}

							@Override
							public void onException(BotApiMethod<Serializable> method, Exception exception) {
								LOG.error(exception.getMessage(), exception);
							}

							@Override
							public void onError(BotApiMethod<Serializable> method,
									TelegramApiRequestException apiException) {
								LOG.error(apiException.getMessage(), apiException);
							}
						});
					}
				}
			}
		}


		public JsonObject getMessageJson(OsmAndAssistantBot bot, Device device, 
				LocationInfo lastSignal, LocationInfo lastLocationSignal) {
			JsonObject obj = new JsonObject();
			updateTime = System.currentTimeMillis();
			obj.addProperty("name", device.deviceName);
			obj.addProperty("id", device.getEncodedId());
			boolean locationCurrentlyPresent = lastSignal.isLocationPresent();
			// display location from lastSignal anyway though it could be deprecated
			if (locationCurrentlyPresent) {
				obj.addProperty("lat", (float) lastSignal.lat);
				obj.addProperty("lon", (float) lastSignal.lon);
				obj.addProperty("locTime", (Long) (lastSignal.timestamp) / 1000);
			} else if (lastLocationSignal != null && lastLocationSignal.isLocationPresent()) {
				obj.addProperty("lat", (float) lastLocationSignal.lat);
				obj.addProperty("lon", (float) lastLocationSignal.lon);
				obj.addProperty("locTime", (Long) (lastLocationSignal.timestamp) / 1000);
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

		private void sendMsg(OsmAndAssistantBot bot, Device device, String txt) {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Hide").setCallbackData("dv|"+
					device.getEncodedId() + "|hide")));
			if (messageId == 0) {
				initialTimestamp = System.currentTimeMillis();
				bot.sendMethodAsync(new SendMessage(chatId, txt).setReplyMarkup(markup).enableHtml(true), new SentCallback<Message>() {

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
				mtd.setText(txt);
				mtd.enableHtml(true);
				mtd.setReplyMarkup(markup);
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
		}
	}


	


	

	
}