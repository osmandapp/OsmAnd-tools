package net.osmand.server.assist;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.osmand.server.assist.convers.AssistantConversation;
import net.osmand.server.assist.convers.RemoveTrackerConversation;
import net.osmand.server.assist.convers.SetTrackerConversation;
import net.osmand.server.assist.convers.UserChatIdentifier;

import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.Update;
import org.telegram.telegrambots.api.objects.User;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.exceptions.TelegramApiException;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

@Component
public class OsmAndAssistantBot extends TelegramLongPollingBot {

	private static final Log LOG = LogFactory.getLog(OsmAndAssistantBot.class);

	PassiveExpiringMap<UserChatIdentifier, AssistantConversation> conversations =
			new PassiveExpiringMap<UserChatIdentifier, AssistantConversation>(5, TimeUnit.MINUTES);

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
		if (!update.hasMessage() || !update.getMessage().hasText()) {
			return;
		}
		Message msg = update.getMessage();
		try {
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
			UserChatIdentifier ucid = new UserChatIdentifier();
			ucid.setChatId(msg.getChatId());
			User from = msg.getFrom();
			if (from != null) {
				ucid.setFirstName(from.getFirstName());
				ucid.setUserId(from.getId() == null ? null : new Long(from.getId().longValue()));
				ucid.setLastName(from.getLastName());
			}

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
					sendAllTrackerConfigurations(ucid);
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
		} catch (TelegramApiException e) {
			LOG.error(
					"Could not send message on update " + msg.getChatId() + " " + msg.getText() + ": " + e.getMessage(),
					e);
		}
	}

	private void setNewConversation(AssistantConversation c) throws TelegramApiException {
		AssistantConversation conversation = conversations.get(c.getChatIdentifier());
		if (conversation != null) {
			sendApiMethod(new SendMessage(c.getChatIdentifier().getChatId(), "FYI: Your conversation about "
					+ conversation.getConversationId() + " was interrupted"));
		}
		conversations.put(c.getChatIdentifier(), c);
	}

	

	public void sendTextMsg(SendMessage msg) throws TelegramApiException {
		sendApiMethod(msg);
	}

	public void saveTrackerConfiguration(TrackerConfiguration config) {
		repository.save(config);
	}

	public void sendAllTrackerConfigurations(UserChatIdentifier ucid) throws TelegramApiException {
		List<TrackerConfiguration> list = repository.findByUserIdOrderByDateCreated(ucid.getChatId());
		StringBuilder bld = new StringBuilder();
		if (list.isEmpty()) {
			bld.append("You don't have any tracker configurations yet. You can set them with /settracker command");
		} else {
			bld.append("Your tracker configurations are:\n");
			for (int i = 0; i < list.size(); i++) {
				TrackerConfiguration c = list.get(i);
				bld.append(i + 1).append(". ").append(c.trackerName).append("\n");
			}
		}
		sendApiMethod(new SendMessage(ucid.getChatId(), bld.toString()));
	}
	
	public TrackerConfiguration removeTrackerConfiguration(UserChatIdentifier ucid, int order) {
		List<TrackerConfiguration> list = repository.findByUserIdOrderByDateCreated(ucid.getUserId());
		TrackerConfiguration config = list.get(order - 1);
		repository.delete(config);
		return config;
	}

	
	/////////////////
	private void sendLocMessage(Message sl, Float lat, Float lon, final int tries) {
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if (tries == 0) {
			return;
		}
		final SentCallback<Serializable> cb = new SentCallback<Serializable>() {

			@Override
			public void onResult(BotApiMethod<Serializable> method, Serializable response) {
				try {
					Thread.sleep(2000);
					System.out.println("Update " + tries);
					sendLocMessage(sl, lat, lon, tries - 1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void onError(BotApiMethod<Serializable> method, TelegramApiRequestException apiException) {
				System.err.println(apiException);
			}

			@Override
			public void onException(BotApiMethod<Serializable> method, Exception exception) {
				System.err.println(exception);
			}

		};
		EditMessageText el = new EditMessageText();
		el.setText((lat + tries * 0.001f) + " " + (lon + tries * 0.001f));
		// EditMessageLiveLocation el = new EditMessageLiveLocation();
		// el.setLatitude(lat + tries * 0.001f);
		// el.setLongitud(lon + tries * 0.001f);
		el.setChatId(sl.getChatId());
		el.setMessageId(sl.getMessageId());
		sendApiMethodAsync(el, cb);
	}

	
}