package net.osmand.server.api.repo;


import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import jakarta.persistence.*;

import com.vladmihalcea.hibernate.type.array.StringArrayType;

import com.google.gson.Gson;
import net.osmand.data.QuadRect;
import net.osmand.server.assist.data.JsonbType;
import org.hibernate.annotations.Type;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.google.gson.JsonObject;

import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;

@Repository
public interface CloudUserFilesRepository extends JpaRepository<UserFile, Long> {
	
	UserFile findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(int userid, String name, String type);
	
	UserFile findTopByUseridAndNameAndTypeAndUpdatetime(int userid, String name, String type, Date updatetime);
	
	UserFile findTopByUseridAndNameAndTypeAndUpdatetimeLessThanOrderByUpdatetimeDesc(int userid, String name, String type, Date updatetime);
	
	UserFile findTopByUseridAndNameAndTypeAndUpdatetimeGreaterThanOrderByUpdatetimeDesc(int userid, String name, String type, Date updatetime);
	
	List<UserFile> findAllByUseridAndNameAndTypeOrderByUpdatetimeDesc(int userid, String name, String type);

	Iterable<UserFile> findAllByUserid(int userid);

    @Query("SELECT uf FROM UserFile uf "
			+ "WHERE uf.userid = :userid AND uf.name LIKE :folderName% AND uf.type = :type AND (uf.name, uf.updatetime) IN "
			+ "(SELECT uft.name, MAX(uft.updatetime) FROM UserFile uft WHERE uft.userid = :userid GROUP BY uft.name)")
	List<UserFile> findLatestFilesByFolderName(@Param("userid") int userid, @Param("folderName") String folderName, @Param("type") String type);

	@Query("""
			   SELECT uf FROM UserFile uf
			   WHERE uf.userid = :userid AND uf.name = :name AND uf.type = :type AND uf.filesize > 0
			   AND uf.details is not null
			   ORDER BY uf.updatetime DESC
			""")
	UserFile findLatestNonEmptyFile(int userid, String name, String type);

//	@Modifying
//	@Query("update UserFile uf set uf.details = ?1 where uf.id = ?2")
//	@Transactional
//	int updateUserFileDetails(JsonObject details, long id);
	
    @Entity(name = "UserFile")
    @Table(name = "user_files")
    class UserFile implements Serializable {
	    private static final Gson gson = new Gson();

	    @Serial
	    private static final long serialVersionUID = 1L;

    	@Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        public long id;
        
        @Column(name = "userid")
        public int userid;
        
        @Column(name = "deviceid")
        public int deviceid;

        @Column(name = "type")
        public String type;
        
        @Column(name = "name")
        public String name;

        @Column(name = "filesize")
        public Long filesize;
        
        @Column(name = "zipfilesize")
        public Long zipfilesize;
        
        @Column(name = "updatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date updatetime;
        
        @Column(name = "clienttime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date clienttime;
        
        @Column(name = "storage")
        public String storage;
        
        @Column(name = "gendetails", columnDefinition = "jsonb")
        @Type(JsonbType.class)
        public JsonObject details;

	    @Column(name = "shortlinktiles", columnDefinition = "text[]")
	    @Type(StringArrayType.class)
	    public String[] shortlinktiles;

	    @Column(name = "data", columnDefinition = "bytea")
	    public byte[] data;

	    @Serial
	    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		    out.defaultWriteObject();
		    out.writeObject(details != null ? gson.toJson(details) : null);
	    }

	    @Serial
	    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		    in.defaultReadObject();
		    String json = (String) in.readObject();
		    if (json != null) {
			    this.details = gson.fromJson(json, JsonObject.class);
		    }
	    }

		public QuadRect getBbox() {
			if (details != null) {
				JsonObject bbox = details.getAsJsonObject("bbox");
				if (bbox != null) {
					return new QuadRect(bbox.get("left").getAsDouble(), bbox.get("top").getAsDouble(),
							bbox.get("right").getAsDouble(), bbox.get("bottom").getAsDouble());
				}
			}
			return null;
		}
    }

    
    // COALESCE(length(u.data), -1))
	@Query("select new net.osmand.server.api.repo.CloudUserFilesRepository$UserFileNoData("
			+ " u.id, u.userid, u.deviceid, u.type, u.name, u.updatetime, u.clienttime, u.filesize, u.zipfilesize, u.storage ) "
			+ " from UserFile u "
			+ " where u.userid = :userid  and (:name is null or u.name = :name) and (:type is null or u.type  = :type ) "
			+ " order by updatetime desc")
	List<UserFileNoData> listFilesByUserid(@Param(value = "userid") int userid,
			@Param(value = "name") String name, @Param(value = "type") String type);
	
	@Query("select new net.osmand.server.api.repo.CloudUserFilesRepository$UserFileNoData("
			+ " u.id, u.userid, u.deviceid, u.type, u.name, u.updatetime, u.clienttime, u.filesize, u.zipfilesize, u.storage, u.details ) "
			+ " from UserFile u "
			+ " where u.userid = :userid  and (:name is null or u.name = :name) and (:type is null or u.type  = :type ) "
			+ " order by updatetime desc")
	List<UserFileNoData> listFilesByUseridWithDetails(@Param(value = "userid") int userid, 
			@Param(value = "name") String name, @Param(value = "type") String type);

	// file used to be transmitted to client as is
	class UserFileNoData {
		public int userid;
		public long id;
        public int deviceid;
        public long filesize;
        public String type;
        public String name;
        public Date updatetime;
        public long updatetimems;
        public Date clienttime;
        public long clienttimems;
		public long zipSize;
		public String storage;
		public JsonObject details;
		public String deviceInfo;
		
		public UserFileNoData(UserFile c) {
			this.userid = c.userid;
			this.id = c.id;
			this.deviceid = c.deviceid;
			this.type = c.type;
			this.name = c.name;
			this.filesize = c.filesize ;
			this.zipSize = c.zipfilesize;
			this.updatetime = c.updatetime;
			this.updatetimems = updatetime == null ? 0 : updatetime.getTime();
			this.clienttime = c.clienttime;
			this.clienttimems = clienttime == null? 0 : clienttime.getTime();
			this.details = c.details;
			this.storage = c.storage;
			this.deviceInfo = null;
		}
		
		public UserFileNoData(long id, int userid, int deviceid, String type, String name, 
				Date updatetime, Date clienttime, Long filesize, Long zipSize, String storage) {
			this(id, userid, deviceid, type, name, updatetime, clienttime, filesize, zipSize, storage, null);
		}
		
		public UserFileNoData(long id, int userid, int deviceid, String type, String name, 
				Date updatetime, Date clienttime, Long filesize, Long zipSize, String storage, JsonObject details) {
			this.userid = userid;
			this.id = id;
			this.deviceid = deviceid;
			this.type = type;
			this.name = name;
			this.filesize = filesize == null ? 0 : filesize.longValue();
			this.zipSize = zipSize == null ? 0 : zipSize.longValue();
			this.updatetime = updatetime;
			this.updatetimems = updatetime == null ? 0 : updatetime.getTime();
			this.clienttime = clienttime;
			this.clienttimems = clienttime == null? 0 : clienttime.getTime();
			this.details = details;
			this.storage = storage;
			this.deviceInfo = null;
		}
	}
	
}
