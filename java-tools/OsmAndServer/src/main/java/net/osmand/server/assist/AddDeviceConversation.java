package net.osmand.server.assist;

import java.util.ArrayList;

import net.osmand.server.assist.data.Device;
import net.osmand.server.assist.data.TrackerConfiguration;
import net.osmand.server.assist.data.UserChatIdentifier;
import net.osmand.server.assist.ext.ITrackerManager;

import org.apache.commons.logging.LogFactory;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.exceptions.TelegramApiException;

import com.google.gson.JsonParser;

public class AddDeviceConversation extends AssistantConversation {
	public static final int ASK_DEVICE_NAME = 0;
	public static final int READ_DEVICE_NAME = 1;
	public static final int READ_EXTERNAL_CONFIGURATION = 2;
	public static final int READ_EXTERNAL_TRACKER_TOKEN = 3;
	public static final int SELECT_EXTERNAL_DEVICE = 3;
	public static final int FINISHED = 10;

	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(AddDeviceConversation.class);
	int state = 0;
	
	private ITrackerManager mgr;

	public AddDeviceConversation(UserChatIdentifier chatIdentifier) {
		super(chatIdentifier);
	}

	@Override
	public String getConversationId() {
		return "add_device";
	}
	

	@Override
	public boolean updateMessage(OsmAndAssistantBot bot, Message msg, String reply) throws TelegramApiException {
		if (state == ASK_DEVICE_NAME) {
			SendMessage smsg = getSendMessage("Please enter a device name:");
			bot.sendMethod(smsg);
			state = READ_DEVICE_NAME;
			return false;
		} else if (state == READ_DEVICE_NAME) {
			if ("import".equals(reply)) {
				SendMessage smsg = getSendMessage("Enter or select external website configuration:");
				InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
				smsg.setReplyMarkup(markup);
				int i = 1;
				for (TrackerConfiguration c : bot.getExternalConfigurations(chatIdentifier)) {
					StringBuilder sb = new StringBuilder();
					sb.append(i).append(". ").append(c.trackerName);
					ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
					InlineKeyboardButton button = new InlineKeyboardButton(sb.toString());
					button.setCallbackData("impc|" + c.trackerName);
					lt.add(button);
					markup.getKeyboard().add(lt);
					i++;
				}
				
				bot.sendMethod(smsg);
				state = READ_EXTERNAL_CONFIGURATION;
			} else if (validateEmptyInput(bot, reply)) {
				Device device = bot.createDevice(chatIdentifier, reply);
				bot.saveDevice(device);
				state = FINISHED;
				bot.retrieveDevices(chatIdentifier, "");
				return true;
			}
			return false;
		} else if (state == READ_EXTERNAL_CONFIGURATION) {
			if (validateEmptyInput(bot, reply)) {
				if (reply.startsWith("https://")) {
					reply = reply.substring("https://".length());
				}
				if (reply.startsWith("http://")) {
					reply = reply.substring("http://".length());
				}
				ITrackerManager cfg = bot.getTrackerManagerById(reply);
				if (cfg != null) {
					SendMessage smsg = getSendMessage("Tracker '" + reply + "' is accepted. " + "Enter API token: ");
					bot.sendMethod(smsg);
					mgr = cfg;
					state = READ_EXTERNAL_TRACKER_TOKEN;
				} else {
					SendMessage smsg = getSendMessage("Configuration '" + reply + "' currently is not supported.");
					bot.sendMethod(smsg);
				}
				return false;
			}
		} else if(state == READ_EXTERNAL_TRACKER_TOKEN) {
			if(validateEmptyInput(bot, reply)) {
				TrackerConfiguration ext = saveConfiguration(bot, reply);
				if(ext == null) {
					return false;
				}
				bot.importFromConfigurationMessage(chatIdentifier, ext);
				state = FINISHED;
				return true;
			}
		} else {
			return true;
		}
		return false;
	}

	private TrackerConfiguration saveConfiguration(OsmAndAssistantBot bot, String token) throws TelegramApiException {
		TrackerConfiguration ext = new TrackerConfiguration();
		ext.mgr = mgr;
		ext.trackerId = mgr.getTrackerId();
		String js = chatIdentifier.getUserJsonString();
		ext.data.add(Device.USER_INFO, new JsonParser().parse(js));
		ext.token = token;
		ext.userId = chatIdentifier.getUserId();
		ext.chatId = chatIdentifier.getChatId();
		mgr.init(ext);
		String err = bot.saveTrackerConfiguration(ext);
		if(err != null) {
			bot.sendMethod(getSendMessage(err));
			return null;
		}
		return ext;
	}

	
	
}
