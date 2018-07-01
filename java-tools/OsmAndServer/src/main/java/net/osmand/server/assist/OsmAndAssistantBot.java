package net.osmand.server.assist;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import net.osmand.server.assist.convers.AssistantConversation;
import net.osmand.server.assist.convers.RemoveTrackerConversation;
import net.osmand.server.assist.convers.SetTrackerConversation;
import net.osmand.server.assist.convers.UserChatIdentifier;
import net.osmand.server.assist.tracker.MegaGPSTracker;

import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.CallbackQuery;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.Update;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.exceptions.TelegramApiException;

import com.google.gson.JsonObject;

@Component
public class OsmAndAssistantBot extends TelegramLongPollingBot {

	private static final Log LOG = LogFactory.getLog(OsmAndAssistantBot.class);

	private static final int LIMIT_CONFIGURATIONS = 3;

	PassiveExpiringMap<UserChatIdentifier, AssistantConversation> conversations =
			new PassiveExpiringMap<UserChatIdentifier, AssistantConversation>(5, TimeUnit.MINUTES);
	
	@Autowired
	MegaGPSTracker megaGPSTracker;

	@Autowired
	TrackerConfigurationRepository repository;

	@Override
	public String getBotUsername() {
		return "osmand_bot";
	}

	public boolean isTokenPresent() {
		return getBotToken() != null;
	}

	@Override
	public String getBotToken() {
		return System.getenv("OSMAND_ASSISTANT_BOT_TOKEN");
	}

	@Override
	public void onUpdateReceived(Update update) {
		Message msg = null;
		try {
			CallbackQuery callbackQuery = update.getCallbackQuery();
			if (callbackQuery != null) {
				String data = callbackQuery.getData();
				UserChatIdentifier ucid = new UserChatIdentifier();
				msg = callbackQuery.getMessage();
				ucid.setChatId(msg.getChatId());
				ucid.setFieldsFromMessage(callbackQuery.getFrom());
				if (!processStandardData(ucid, data, callbackQuery)) {
					AssistantConversation conversation = conversations.get(ucid);
					if (conversation != null) {
						boolean finished = conversation.updateMessage(this, msg, data);
						if (finished) {
							conversations.remove(ucid);
						}
					}
				}
			} else if (update.hasMessage() && update.getMessage().hasText()) {
				msg = update.getMessage();
				SendMessage snd = new SendMessage();
				snd.setChatId(msg.getChatId());
				String coreMsg = msg.getText();
				if (coreMsg.startsWith("/")) {
					coreMsg = coreMsg.substring(1);
				}
				int at = coreMsg.indexOf("@");
				if (at > 0) {
					coreMsg = coreMsg.substring(0, at);
				}
				String params = "";
				if (msg.getText().indexOf(' ') != -1) {
					params = msg.getText().substring(msg.getText().indexOf(' ')).trim();
				}
				UserChatIdentifier ucid = new UserChatIdentifier();
				ucid.setChatId(msg.getChatId());
				ucid.setFieldsFromMessage(msg.getFrom());

				if (msg.isCommand()) {
					if ("settracker".equals(coreMsg)) {
						SetTrackerConversation nconversation = new SetTrackerConversation(ucid);
						setNewConversation(nconversation);
						nconversation.updateMessage(this, msg);
					} else if ("removetracker".equals(coreMsg)) {
						RemoveTrackerConversation nconversation = new RemoveTrackerConversation(ucid);
						setNewConversation(nconversation);
						nconversation.updateMessage(this, msg);
					} else if ("mytrackers".equals(coreMsg)) {
						sendAllTrackerConfigurations(ucid, false);
					} else if ("mydevices".equals(coreMsg)) {
						retrieveMyDevices(ucid, params);
					} else if ("whoami".equals(coreMsg)) {
						sendApiMethod(new SendMessage(msg.getChatId(), "I'm your OsmAnd assistant"));
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
			if (msg != null) {
				LOG.error(
						"Could not send message on update " + msg.getChatId() + " " + msg.getText() + ": "
								+ e.getMessage(), e);
			}
		}
	}


	private boolean processStandardData(UserChatIdentifier ucid, String data, CallbackQuery callbackQuery) {
		if(data.startsWith("device|")) {
			String[] sl = data.split("\\|");
			int cId = Integer.parseInt(sl[1]);
			Optional<TrackerConfiguration> tracker = repository.findById(new Long(cId));
			if(tracker.isPresent()) {
				TrackerConfiguration c = tracker.get();
				if(c.userId.longValue() == ucid.getUserId().longValue() ||
						(c.data.has(TrackerConfiguration.CHATS) && 
						c.data.get(TrackerConfiguration.CHATS).getAsJsonObject().has(ucid.getChatId()+""))) {
					if(megaGPSTracker.accept(c)) {
						megaGPSTracker.retrieveInfoAboutMyDevice(this, ucid, c, sl[2]);
					}
				} else {
					throw new IllegalStateException("User reply is corrupted");
				}
			}
			return true;
		}
		return false;
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

	public String saveTrackerConfiguration(TrackerConfiguration config) {
		List<TrackerConfiguration> list = repository.findByUserIdOrderByDateCreated(config.userId);
		if(list.size() >= LIMIT_CONFIGURATIONS) {
			return "Currently 1 user is allowed to have only " + LIMIT_CONFIGURATIONS + " configurations.";
		}
		repository.save(config);
		return null;
	}

	public void sendAllTrackerConfigurations(UserChatIdentifier ucid, boolean keyboard) throws TelegramApiException {
		List<TrackerConfiguration> list = repository.findByUserIdOrderByDateCreated(ucid.getUserId());
		StringBuilder bld = new StringBuilder();
		SendMessage msg = new SendMessage();
		msg.setChatId(ucid.getChatId());
		if (list.isEmpty()) {
			bld.append("You don't have any tracker configurations yet. You can set them with /settracker command");
		} else {
			InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
			bld.append("Your tracker configurations are:\n");
			for (int i = 0; i < list.size(); i++) {
				
				TrackerConfiguration c = list.get(i);
				String txt = (i+1) +". " + c.trackerName;
				if (!keyboard) {
					bld.append(txt).append("\n");
				} else {
					ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
					InlineKeyboardButton button = new InlineKeyboardButton(txt);
					button.setCallbackData((i + 1) + "");
					lt.add(button);
					markup.getKeyboard().add(lt);
				}
			}
			msg.setReplyMarkup(markup);
			
		}
		msg.setText(bld.toString());
		sendApiMethod(msg);
	}
	
	public TrackerConfiguration removeTrackerConfiguration(UserChatIdentifier ucid, int order) {
		List<TrackerConfiguration> list = repository.findByUserIdOrderByDateCreated(ucid.getUserId());
		TrackerConfiguration config = list.get(order - 1);
		repository.delete(config);
		return config;
	}

	
	public void retrieveMyDevices(UserChatIdentifier ucid, String params) throws TelegramApiException {
		List<TrackerConfiguration> list = repository.findByUserIdOrderByDateCreated(ucid.getUserId());
		if (list.isEmpty()) {
			sendApiMethod(new SendMessage(ucid.getChatId(), "You don't have any tracking configurations set yet"));
		} else {
			int i = 1;
			String cid = ucid.getChatId() + "";
			for (TrackerConfiguration c : list) {
				if(!c.data.has(TrackerConfiguration.CHATS) || 
						!c.data.get(TrackerConfiguration.CHATS).getAsJsonObject().has(cid)) {
					if(!c.data.has(TrackerConfiguration.CHATS)) {
						c.data.add(TrackerConfiguration.CHATS, new JsonObject());
					}
					c.data.get(TrackerConfiguration.CHATS).getAsJsonObject().add(cid, new JsonObject());
					repository.save(c);
				}
				if(params.isEmpty() || params.equals(i+"")) {
					retrieveMyDevices(c, ucid, list.size() == 1 ? 0 : 1);
				}
				i++;
			}
		}
	}

	private void retrieveMyDevices(TrackerConfiguration c, UserChatIdentifier ucid, int cfgOrder) throws TelegramApiException {
		if(megaGPSTracker.accept(c)) {
			megaGPSTracker.retrieveMyDevices(this, c, ucid, cfgOrder);
		} else {
			sendApiMethod(new SendMessage(ucid.getChatId(), "Tracker configuration '" + c.trackerId
					+ "' is not supported"));
		}
	}
	
}