package net.osmand.server.assist;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.api.methods.AnswerInlineQuery;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.CallbackQuery;
import org.telegram.telegrambots.api.objects.Location;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.Update;
import org.telegram.telegrambots.api.objects.inlinequery.InlineQuery;
import org.telegram.telegrambots.api.objects.inlinequery.inputmessagecontent.InputLocationMessageContent;
import org.telegram.telegrambots.api.objects.inlinequery.inputmessagecontent.InputTextMessageContent;
import org.telegram.telegrambots.api.objects.inlinequery.result.InlineQueryResult;
import org.telegram.telegrambots.api.objects.inlinequery.result.InlineQueryResultArticle;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.exceptions.TelegramApiException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import net.osmand.server.assist.data.Device;
import net.osmand.server.assist.data.DeviceBean;
import net.osmand.server.assist.data.LocationInfo;
import net.osmand.server.assist.data.UserChatIdentifier;
import net.osmand.util.MapUtils;

@Component
public class OsmAndAssistantBot extends TelegramLongPollingBot {

	public static final String URL_TO_POST_COORDINATES = "https://live.osmand.net/device/%s/send";
	
	private static final Log LOG = LogFactory.getLog(OsmAndAssistantBot.class);
	SimpleDateFormat UTC_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	{
		UTC_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	SimpleDateFormat UTC_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
	{
		UTC_TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	PassiveExpiringMap<UserChatIdentifier, AssistantConversation> conversations =
			new PassiveExpiringMap<UserChatIdentifier, AssistantConversation>(5, TimeUnit.MINUTES);
	
	@Autowired
	DeviceLocationManager deviceLocManager;
	
	@Override
	public String getBotUsername() {
		if(System.getenv("OSMAND_DEV_TEST_BOT_TOKEN") != null) {
			return System.getenv("OSMAND_DEV_TEST_BOT"); 
		}
		return "osmand_bot";
	}

	public boolean isTokenPresent() {
		return getBotToken() != null;
	}

	@Override
	public String getBotToken() {
		if(System.getenv("OSMAND_DEV_TEST_BOT_TOKEN") != null) {
			return System.getenv("OSMAND_DEV_TEST_BOT_TOKEN");
		}
		return System.getenv("OSMAND_ASSISTANT_BOT_TOKEN");
	}
	
	private static class ChatCommandParams {

		public String coreMsg;
		public String params;
		
	}

	@Override
	public void onUpdateReceived(Update update) {
		Message msg = null;
		UserChatIdentifier ucid = new UserChatIdentifier();
		try {
			
			CallbackQuery callbackQuery = update.getCallbackQuery();
			InlineQuery iq = update.getInlineQuery();
			if (iq != null) {
				processInlineQuery(iq);
			} else if (callbackQuery != null) {
				msg = callbackQuery.getMessage();
				if(msg != null) {
					ucid.setChatId(msg.getChatId());
				}
				ucid.setUser(callbackQuery.getFrom());
				String data = callbackQuery.getData();
				processCallback(ucid, data, callbackQuery);
			} else if (update.hasMessage() && update.getMessage().hasText()) {
				msg = update.getMessage();
				ucid.setChatId(msg.getChatId());
				ucid.setUser(msg.getFrom());
				SendMessage snd = new SendMessage();
				snd.setChatId(msg.getChatId());
				ChatCommandParams cmd = parseCommandParams(msg);
				if (msg.isCommand()) {
					if ("add_device".equals(cmd.coreMsg)) {
						AddDeviceConversation c = new AddDeviceConversation(ucid);
						setNewConversation(c);
						c.updateMessage(this, msg);
					} else if ("mydevices".equals(cmd.coreMsg) || 
							("start".equals(cmd.coreMsg) && "mydevices".equals(cmd.params))) {
						retrieveDevices(ucid, cmd.params, 0);
					} else if ("whoami".equals(cmd.coreMsg) || "start".equals(cmd.coreMsg)) {
						sendApiMethod(new SendMessage(msg.getChatId(), "I'm your OsmAnd assistant. To list your devices use /mydevices."));
					} else {
						sendApiMethod(new SendMessage(msg.getChatId(), "Sorry, the command is not recognized"));
					}
				} else {
					AssistantConversation conversation = conversations.get(ucid);
					if (conversation != null) {
						boolean finished = conversation.updateMessage(this, msg);
						if (finished) {
							conversations.remove(ucid);
						}
					}
				}
			}
		} catch (Exception e) {
			if (!(e instanceof TelegramApiException)) {
				try {
					sendMethod(new SendMessage(ucid.getChatId(), "Internal error: " + e.getMessage()));
				} catch (TelegramApiException e1) {
				}
			}
			LOG.warn(e.getMessage(), e);
		}
	}

	private ChatCommandParams parseCommandParams(Message msg) {
		ChatCommandParams cmd = new ChatCommandParams();
		cmd.coreMsg = msg.getText();
		if (cmd.coreMsg.startsWith("/")) {
			cmd.coreMsg = cmd.coreMsg.substring(1);
		}
		cmd.params = "";
		int space = cmd.coreMsg.indexOf(' ');
		if (space != -1) {
			cmd.params = cmd.coreMsg.substring(space).trim();
			cmd.coreMsg = cmd.coreMsg.substring(0, space);
			
		}
		int at = cmd.coreMsg.indexOf("@");
		if (at > 0) {
			cmd.coreMsg = cmd.coreMsg.substring(0, at);
		}
		return cmd;
	}

	private void processInlineQuery(InlineQuery iq) throws TelegramApiException {
		AnswerInlineQuery aw = new AnswerInlineQuery();
		aw.setInlineQueryId(iq.getId());
		aw.setPersonal(true);
		aw.setCacheTime(15);
		String query = iq.getQuery();
		List<InlineQueryResult> results = new ArrayList<InlineQueryResult>();
		List<Device> devs = deviceLocManager.getDevicesByUserId(this, iq.getFrom().getId());
		Location userLoc = iq.getLocation();
		for(Device d : devs) {
			LocationInfo l = d.getLastLocationSignal();
			if(!query.isEmpty() && !d.getDeviceName().toLowerCase().startsWith(query.toLowerCase()) ) {
				continue;
			}
			if (l != null) {
				String desc = String.format("Location updated: %s", formatTime(l.getTimestamp(), false));
					if (userLoc != null && l.isLocationPresent()) {
						desc += String.format(
							"Distance: %d meters",
							(int) MapUtils.getDistance(l.getLat(), l.getLon(), userLoc.getLatitude(),
									userLoc.getLongitude()));
					}
				InlineQueryResultArticle txt = new InlineQueryResultArticle();
				txt.setDescription(desc);
				if(!query.isEmpty()) {
					txt.setDescription("Location will be displayed as text and could be viewed in OsmAnd");
				}
				txt.setId("t"+d.getStringId());
				txt.setTitle(d.getDeviceName());
				txt.setInputMessageContent(new InputTextMessageContent().setMessageText(
						d.getMessageTxt(0)).enableHtml(true));
				InlineKeyboardMarkup mk = new InlineKeyboardMarkup();
				InlineKeyboardButton btn = new InlineKeyboardButton("Start update").setCallbackData(
						"msg|"+d.getStringId()+"|starttxt");
				mk.getKeyboard().add(Collections.singletonList(btn));
				txt.setReplyMarkup(mk);
				results.add(txt);
				if (l.isLocationPresent()) {
					InlineQueryResultArticle loc = new InlineQueryResultArticle();
					loc.setDescription(desc);
					if(!query.isEmpty()) {
						loc.setDescription("Location will be displayed on the map. Only 1 message is allowed per chat.");
					}
					loc.setId("m" + d.getStringId());
					loc.setTitle(d.getDeviceName() + " on map");
					loc.setInputMessageContent(new InputLocationMessageContent((float) l.getLat(), (float) l.getLon())
							.setLivePeriod(DeviceLocationManager.DEFAULT_LIVE_PERIOD));
					mk = new InlineKeyboardMarkup();
					btn = new InlineKeyboardButton("Start update").setCallbackData("msg|" + d.getStringId()
							+ "|startmap");
					mk.getKeyboard().add(Collections.singletonList(btn));
					loc.setReplyMarkup(mk);
					results.add(loc);
				}
			}
		}
		aw.setSwitchPmParameter("mydevices");
		aw.setSwitchPmText("Your devices");
		aw.setResults(results);
		sendApiMethod(aw);
	}


	protected boolean processCallback(UserChatIdentifier ucid, String data, CallbackQuery callbackQuery) throws TelegramApiException {
		Message msg = callbackQuery.getMessage();
		String[] sl = data.split("\\|");
		if(msg == null) {
			// process inline message id
			String inlineMsgId = callbackQuery.getInlineMessageId();
			if(data.startsWith("msg|")) {
				String pm = sl.length > 2 ? sl[2] : "";
				Device d = deviceLocManager.getDevice(this, sl[1]);
				if(pm.equals("starttxt") || pm.equals("updtxt")) {
					d.showLiveMessage(inlineMsgId);
				} else if(pm.equals("startmap") || pm.equals("updmap")) {
					d.showLiveMap(inlineMsgId);
				} else if(pm.equals("hide")) {
					d.hideInlineMsg(inlineMsgId);
				}
				return true;
			}
			return false;
		}
		if(data.equals("hide")) {
			DeleteMessage del = new DeleteMessage(msg.getChatId(), msg.getMessageId());
			sendApiMethod(del);
			return true;
		}
		
		UserChatIdentifier ci = new UserChatIdentifier();
		ci.setChatId(msg.getChatId());
		ci.setUser(callbackQuery.getFrom());
		if(data.startsWith("devices|")) {
			if(data.startsWith("devices|add")) {
				AddDeviceConversation c = new AddDeviceConversation(ucid);
				setNewConversation(c);
				c.updateMessage(this, msg);
				return true;
			}
			retrieveDevices(ucid, "", msg.getMessageId());
			return true;
		} else if(data.startsWith("dv|")) {
			String pm = sl.length > 2 ? sl[2] : "";
			Device d = deviceLocManager.getDevice(this, sl[1]);
			if(d == null) {
				SendMessage snd = new SendMessage();
				snd.setChatId(msg.getChatId());
				snd.setText("Device is not found.");
				sendApiMethod(snd);
			} else {
				if(callbackQuery.getFrom() == null || d.getOwnerId() != callbackQuery.getFrom().getId()){
					SendMessage snd = new SendMessage(msg.getChatId(), "Only device owner can request information about device.");
					sendApiMethod(snd);	
				} else {
					retrieveDeviceInformation(d, msg, pm);
				}
			}
		} else {
			AssistantConversation conversation = conversations.get(ucid);
			if (conversation != null) {
				boolean finished = conversation.updateMessage(this, msg, data);
				if (finished) {
					conversations.remove(ucid);
				}
			}
			return true;
		}
		return false;
	}

	public void retrieveDeviceInformation(Device d, 
			Message msg, String pm) throws TelegramApiException {

		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
		ArrayList<InlineKeyboardButton> lt2 = new ArrayList<InlineKeyboardButton>();
		ArrayList<InlineKeyboardButton> lt3 = new ArrayList<InlineKeyboardButton>();
		markup.getKeyboard().add(lt);
		markup.getKeyboard().add(lt2);
		markup.getKeyboard().add(lt3);
		LocationInfo sig = d.getLastLocationSignal();
		if(pm.equals("hide")) {
			d.hideMessage(msg.getChatId(), msg.getMessageId());
			sendApiMethod(new DeleteMessage(msg.getChatId(), msg.getMessageId()));
		} else if(pm.equals("more") && !msg.getChat().isUserChat()) {
			sendApiMethod(new SendMessage(msg.getChatId(), "Detailed information could not be displayed in a non-private chat."));
			return;
		} else if(pm.equals("delconfirm")) {
			EditMessageText editMsg = new EditMessageText();
			editMsg.setChatId(msg.getChatId());
			editMsg.setMessageId(msg.getMessageId());
			editMsg.enableHtml(true).setReplyMarkup(markup).setText("Device was deleted.");
			deviceLocManager.delete(d);
			sendApiMethod(editMsg);
			return;
		} else if(pm.equals("mon")) {
			d.startMonitoring();
		} else if(pm.equals("stmon")) {
			d.stopMonitoring();
		} else if (pm.equals("loctext")) {
			d.showLiveMessage(msg.getChatId());
		} else if (pm.equals("loc")) {
			d.showLiveMap(msg.getChatId());
		}
		String locMsg = formatLocation(sig, false);
		String txt = String.format("<b>Device</b>: %s\n<b>Location</b>: %s\n", d.getDeviceName(), locMsg);
		if(pm.equals("del")) {
			txt += "<b>Are you sure, you want to delete it? </b>";
		} else if(pm.equals("more")) {
			txt = "<b>ID</b>:" + d.getStringId() + "\n" + txt; 
			txt += String.format("\n<b>URL to post location</b>: %s\n<b>Registered</b>: %s\n", 
					String.format(URL_TO_POST_COORDINATES, d.getStringId()), UTC_DATE_FORMAT.format(d.getCreatedDate()));
		} else {
			
			txt += String.format("\nTime: %s UTC\n", UTC_TIME_FORMAT.format(new Date()));
		}
		
		
		if(pm.equals("del")) {
			InlineKeyboardButton upd = new InlineKeyboardButton("No");
			upd.setCallbackData("dv|" + d.getStringId() + "|less");
			lt.add(upd);

			InlineKeyboardButton more = new InlineKeyboardButton("Yes");
			more.setCallbackData("dv|" + d.getStringId() + "|delconfirm");
			lt.add(more);			
		} else {
			if (pm.equals("more")) {
				InlineKeyboardButton ls = new InlineKeyboardButton("Less");
				ls.setCallbackData("dv|" + d.getStringId() + "|less");
				lt.add(ls);
				InlineKeyboardButton del = new InlineKeyboardButton("Delete");
				del.setCallbackData("dv|" + d.getStringId() + "|del");
				lt.add(del);
			} else {
				InlineKeyboardButton hide = new InlineKeyboardButton("Hide");
				hide.setCallbackData("hide");
				lt.add(hide);
				InlineKeyboardButton upd = new InlineKeyboardButton("Update");
				upd.setCallbackData("dv|" + d.getStringId() + "|upd");
				lt.add(upd);
				InlineKeyboardButton more = new InlineKeyboardButton("More");
				more.setCallbackData("dv|" + d.getStringId() + "|more");
				lt.add(more);
				if (sig != null && sig.isLocationPresent()) {
					InlineKeyboardButton locT = new InlineKeyboardButton("Live text");
					locT.setCallbackData("dv|" + d.getStringId() + "|loctext");
					lt2.add(locT);
					lt2.clear();
					
					InlineKeyboardButton locK = new InlineKeyboardButton("Location on map");
					locK.setCallbackData("dv|" + d.getStringId() + "|loc");
					lt2.add(locK);
					
					
//					
					
					InlineKeyboardButton shareMap = new InlineKeyboardButton("Share");
					shareMap.setSwitchInlineQuery(d.getDeviceName());
					lt2.add(shareMap);
					
					
				}
			}
		}
		
		if (pm.equals("")) {
			SendMessage sendMessage = new SendMessage();
			sendMessage.setChatId(msg.getChatId());
			sendMessage.enableHtml(true).setReplyMarkup(markup).setText(txt);
			sendApiMethod(sendMessage);
		} else {
			EditMessageText editMsg = new EditMessageText();
			editMsg.setChatId(msg.getChatId());
			editMsg.setMessageId(msg.getMessageId());
			editMsg.setReplyMarkup(markup);
			editMsg.enableHtml(true).setText(txt);
			sendApiMethod(editMsg);
		}
	}

	public String formatLocation(LocationInfo sig, boolean now) {
		String locMsg = "n/a";
		if (sig != null && sig.isLocationPresent()) {
			locMsg = String.format("%.5f, %.5f (%s)", sig.getLat(), sig.getLon(),
					formatTime(sig.getTimestamp(), now));
		}
		return locMsg;
	}


	private void setNewConversation(AssistantConversation c) throws TelegramApiException {
		AssistantConversation conversation = conversations.get(c.getChatIdentifier());
		if (conversation != null) {
			sendApiMethod(new SendMessage(c.getChatIdentifier().getChatId(), "FYI: Your conversation about "
					+ conversation.getConversationId() + " was interrupted"));
		}
		conversations.put(c.getChatIdentifier(), c);
	}

	public final <T extends Serializable, Method extends BotApiMethod<T>> T sendMethod(Method method) throws TelegramApiException {
		return super.sendApiMethod(method);
    }
	
	public final <T extends Serializable, Method extends BotApiMethod<T>, Callback extends SentCallback<T>> void sendMethodAsync(Method method, Callback callback) {
		super.sendApiMethodAsync(method, callback);
    }
	
	public void saveDeviceInfo(DeviceBean device) {
		deviceLocManager.saveNoCheck(device);
		
	}

	
	
	public void retrieveDevices(UserChatIdentifier ucid, String params, int messageId) throws TelegramApiException {
		int i = 1;
		parseRecentParam(params);
		List<Device> devs = deviceLocManager.getDevicesByUserId(this, ucid.getUserId()); 
		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		String txt =  "You don't have any added devices. Please use /add_device to register new devices.";
		if (devs.size() > 0) {
			int monitored = 0;
			int live = 0;
			for (Device c : devs) {
				StringBuilder sb = new StringBuilder();
				sb.append(i).append(". ").append(c.getDeviceName());
				LocationInfo last = c.getLastLocationSignal();
				if(last != null && last.isLocationPresent()) {
					if(System.currentTimeMillis() - last.getTimestamp() < TimeUnit.HOURS.toMillis(1)) {
						sb.append(" - LIVE ");
						live++;
					}
				}
				if(c.isLocationMonitored()) {
					sb.append(" [monitored] ");
					monitored++;
				}
				
				InlineKeyboardButton button = new InlineKeyboardButton(sb.toString());
				button.setCallbackData("dv|" + c.getStringId());
				markup.getKeyboard().add(Collections.singletonList(button));
				i++;
			}
			txt = String.format("<b>Available devices</b>: %d (%d active)", devs.size(), live);
			if(monitored > 0) {
				txt += "\n"  + String.format("<b>Actively monitored devices</b>: %d", monitored);
			}
			txt += String.format("\nTime: %s UTC\n", UTC_TIME_FORMAT.format(new Date()));
			InlineKeyboardButton hide = new InlineKeyboardButton("Hide").setCallbackData("hide");
			InlineKeyboardButton upd = new InlineKeyboardButton("Update").setCallbackData("devices|update");
			InlineKeyboardButton nw = new InlineKeyboardButton("New").setCallbackData("devices|add");
			markup.getKeyboard().add(Arrays.asList(hide, upd, nw));
		}
		if(messageId == 0) {
			SendMessage msg = new SendMessage();
			msg.setChatId(ucid.getChatId());
			msg.enableHtml(true);
			msg.setReplyMarkup(markup);
			msg.setText(txt);
			sendApiMethod(msg);
		} else {
			EditMessageText msg = new EditMessageText();
			msg.setChatId(ucid.getChatId());
			msg.setMessageId(messageId);
			msg.enableHtml(true);
			msg.setReplyMarkup(markup);
			msg.setText(txt);
			sendApiMethod(msg);
		}
	}

	private long parseRecentParam(String params) {
		long filterLocationTime = 0;
		if (params.startsWith("recent")) {
			long dl = TimeUnit.HOURS.toMillis(24);
			if (params.startsWith("recent ")) {
				try {
					double hours = Double.parseDouble(params.substring("recent ".length()));
					dl = TimeUnit.MINUTES.toMillis((long) (60 * hours));
				} catch (NumberFormatException e) {
				}
			}
			filterLocationTime = (System.currentTimeMillis() - dl) / 1000l;
		}
		return filterLocationTime;
	}
	
	
	
	public String formatTime(long ti, boolean now) {
		Date dt = new Date(ti);
		long current = System.currentTimeMillis() / 1000;
		long tm = ti / 1000;
		if(current - tm < 10) {
			return now ? "now" : "few seconds ago";
		} else if (current - tm < 50) {
			return (current - tm) + " seconds ago";
		} else if (current - tm < 60 * 60 * 2) {
			return (current - tm) / 60 + " minutes ago";
		} else if (current - tm < 60 * 60 * 24) {
			return (current - tm) / (60 * 60) + " hours ago";
		}
		return UTC_DATE_FORMAT.format(dt) + " " + UTC_TIME_FORMAT.format(dt) + " UTC";
	}
	
	public String formatFullTime(long ti) {
		Date dt = new Date(ti);
		return UTC_DATE_FORMAT.format(dt) + " " + UTC_TIME_FORMAT.format(dt) + " UTC";
	}


	public String createNewDevice(UserChatIdentifier chatIdentifier, String name) {
		return deviceLocManager.registerNewDevice(chatIdentifier, name);
	}

	

	
	
}