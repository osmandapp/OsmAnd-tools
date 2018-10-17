package net.osmand.server.assist.data;

import javax.persistence.*;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.StringUtils;
import org.hibernate.annotations.Type;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import com.google.common.primitives.Longs;
import com.google.gson.JsonObject;

import java.util.*;

@Entity
@Table(name = "telegram_devices")
@EntityListeners(AuditingEntityListener.class)
public class DeviceBean {

	@Id
	public long id;
	
	@Column(nullable = false)
	public long userId;
	
	
	@Column(nullable = false)
	public long chatId;
	
	@Column(nullable = false, length = 200, name="device_name")
	public String deviceName;
	
	@JoinColumn(nullable = true, name = "ext_config")
	@ManyToOne(optional = true, fetch = FetchType.EAGER)
	public TrackerConfiguration externalConfiguration;
	
	@Column(nullable = true, length = 200, name="ext_id")
	public String externalId;
	
	@Column(name = "created_date", nullable = false, updatable = false)
	@CreatedDate
	@Temporal(TemporalType.TIMESTAMP)
	public java.util.Date createdDate;

	@Column(name = "modified_date")
	@LastModifiedDate
	@Temporal(TemporalType.TIMESTAMP)
	public java.util.Date modifiedDate;
	
	
	public String getEncodedId() {
		Base32 base32 = new Base32(false);
		byte[] byteArray = Longs.toByteArray(id);
		int offset = 0;
		while(offset < byteArray.length && byteArray[offset] == 0) {
			offset++;
		}
		String msg = StringUtils.newStringUtf8(base32.encode(byteArray, offset, byteArray.length - offset)).
				replace('=', ' ').trim();
		if (getDecodedId(msg) != id) {
			throw new IllegalArgumentException();
		}
		return msg;
	}
	
	public static long getDecodedId(String str) {
		Base32 base32 = new Base32(false);
		byte[] nbytearray = base32.decode(str.getBytes());
		byte[] ls = new byte[8];
		for (int i = 7; i > 0; i--) {
			if (nbytearray.length + i - 8 >= 0) { 
				ls[i] = nbytearray[nbytearray.length + i - 8];
			} else {
				ls[i] = 0;
			}
		}
		return Longs.fromByteArray(ls);
	}

	
	public static final String USER_INFO = "user";
	
	@Column(name = "data", columnDefinition = "jsonb")
    @Type(type = "net.osmand.server.assist.data.JsonbType") 
	public JsonObject data = new JsonObject();

	@OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
	@JoinColumn(name = "device_id")
	public Set<Device.LocationChatMessage> chatMessages = new HashSet<>();
}
