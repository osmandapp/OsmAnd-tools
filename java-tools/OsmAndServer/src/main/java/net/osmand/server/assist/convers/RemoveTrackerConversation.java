package net.osmand.server.assist.convers;

import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.server.assist.TrackerConfiguration;

import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.exceptions.TelegramApiException;

public class RemoveTrackerConversation extends AssistantConversation {

	int state = 0;

	public RemoveTrackerConversation(UserChatIdentifier chatIdentifier) {
		super(chatIdentifier);
	}

	@Override
	public String getConversationId() {
		return "removetracker";
	}

	@Override
	public boolean updateMessage(OsmAndAssistantBot bot, Message msg) throws TelegramApiException {
		if (state == 0) {
			bot.sendAllTrackerConfigurations(msg.getChatId());
			bot.sendTextMsg(getSendMessage("Please specify tracker site to monitor:"));
			state++;
			return false;
		} else if (state == 1) {
			System.out.println(msg.getText());
			return true;
		}
		return true;
	}

	private SendMessage getSendMessage(String text) {
		return new SendMessage(chatIdentifier.getChatId(), text);
	}

}
