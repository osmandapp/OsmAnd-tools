package net.osmand.server.assist.data;

import org.telegram.telegrambots.meta.api.objects.User;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UserChatIdentifier {

	private Long chatId;
	private User user;
	public Long getChatId() {
		return chatId;
	}

	public void setChatId(Long chatId) {
		this.chatId = chatId;
	}
	
	public void setUser(User user) {
		this.user = user;
	}

	public Long getUserId() {
		return user.getId();
	}
	
	public User getUser() {
		return user;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((chatId == null) ? 0 : chatId.hashCode());
		result = prime * result + ((getUserId() == null) ? 0 : getUserId().hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserChatIdentifier other = (UserChatIdentifier) obj;
		if (chatId == null) {
			if (other.chatId != null)
				return false;
		} else if (chatId.longValue() != other.chatId.longValue())
			return false;
		
		if (getUserId() == null) {
			if (other.getUserId() != null)
				return false;
		} else if (!getUserId().equals(other.getUserId()))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "UserChatIdentifier [chatId=" + chatId + ", userId=" + getUserId() + "]";
	}

	public String getUserJsonString() {
		try {
			if (user == null) {
				return "{}";
			}
			ObjectMapper mapper = new ObjectMapper();
			return mapper.writeValueAsString(user);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}


}
