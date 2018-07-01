package net.osmand.server.assist.convers;

import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.server.assist.TrackerConfiguration;

import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.exceptions.TelegramApiException;

public class SetTrackerConversation extends AssistantConversation {
	public static final int ASK_TRACKER_ID = 0;
	public static final int ASK_TRACKER_TOKEN = 1;

	int state = 0;
	TrackerConfiguration config = new TrackerConfiguration();

	public SetTrackerConversation(UserChatIdentifier chatIdentifier) {
		super(chatIdentifier);
	}

	@Override
	public String getConversationId() {
		return "settracker";
	}

	@Override
	public boolean updateMessage(OsmAndAssistantBot bot, Message msg) throws TelegramApiException {
		if (state == ASK_TRACKER_ID) {
			bot.sendTextMsg(getSendMessage("Please specify tracker site to monitor:"));
			config.lastName = chatIdentifier.getLastName();
			config.firstName = chatIdentifier.getFirstName();
			config.userId = chatIdentifier.getUserId();
			state++;
		} else if (state == ASK_TRACKER_TOKEN) {
			config.trackerId = msg.getText();
			bot.sendTextMsg(getSendMessage("Please specify tracker token:"));
			state++;
		} else {
			config.token = msg.getText();
			String suffix = config.token.substring(config.token.length() - 3, config.token.length());
			config.trackerName = config.trackerId + "-" + suffix;
			System.out.println(config);
			bot.saveTrackerConfiguration(config);
			return true;
		}
		return false;
	}

	private SendMessage getSendMessage(String text) {
		return new SendMessage(chatIdentifier.getChatId(), text);
	}

}
