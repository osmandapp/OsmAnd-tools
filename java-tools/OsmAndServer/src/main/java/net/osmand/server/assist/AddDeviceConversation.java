package net.osmand.server.assist;

import org.apache.commons.logging.LogFactory;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.exceptions.TelegramApiException;

import net.osmand.server.assist.data.UserChatIdentifier;

public class AddDeviceConversation extends AssistantConversation {
	public static final int ASK_DEVICE_NAME = 0;
	public static final int READ_DEVICE_NAME = 1;
	public static final int SELECT_EXTERNAL_DEVICE = 3;
	public static final int FINISHED = 10;

	protected static final org.apache.commons.logging.Log LOG = LogFactory.getLog(AddDeviceConversation.class);
	int state = 0;
	

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
			if (validateEmptyInput(bot, reply)) {
				String msgs = bot.createNewDevice(chatIdentifier, reply);
				bot.sendMethod(getSendMessage(msgs));
				state = FINISHED;
				return true;
			}
			return false;
		} else {
			return true;
		}
	}

}
