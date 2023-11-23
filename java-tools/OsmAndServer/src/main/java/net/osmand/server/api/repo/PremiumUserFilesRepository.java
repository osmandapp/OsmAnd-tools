package net.osmand.server.api.repo;


import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Type;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.google.gson.JsonObject;

import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;

@Repository
public interface PremiumUserFilesRepository extends JpaRepository<UserFile, Long> {
	
	UserFile findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(int userid, String name, String type);
	
	UserFile findTopByUseridAndNameAndTypeAndUpdatetime(int userid, String name, String type, Date updatetime);
	
	Iterable<UserFile> findAllByUserid(int userid);
	
	@Query("SELECT uf FROM UserFile uf " +
			"WHERE uf.userid = :userid AND uf.name LIKE :folderName% AND uf.type = :type " +
			"AND uf.updatetime = (SELECT MAX(uft.updatetime) FROM UserFile uft WHERE uft.userid = :userid AND uft.name = uf.name)")
	List<UserFile> findLatestFilesByFolderName(@Param("userid") int userid, @Param("folderName") String folderName, @Param("type") String type);
	
	
//	@Modifying
//	@Query("update UserFile uf set uf.details = ?1 where uf.id = ?2")
//	@Transactional
//	int updateUserFileDetails(JsonObject details, long id);
	
    @Entity(name = "UserFile")
    @Table(name = "user_files")
    class UserFile {

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
        @Type(type = "net.osmand.server.assist.data.JsonbType")
        public JsonObject details;
        
//      @Fetch(FetchMode.JOIN)
        @Column(name = "data", columnDefinition="bytea")
        public byte[] data;
        
//        @Lob
//        public Blob data;

    }

    
    // COALESCE(length(u.data), -1))
	@Query("select new net.osmand.server.api.repo.PremiumUserFilesRepository$UserFileNoData("
			+ " u.id, u.userid, u.deviceid, u.type, u.name, u.updatetime, u.clienttime, u.filesize, u.zipfilesize, u.storage ) "
			+ " from UserFile u "
			+ " where u.userid = :userid  and (:name is null or u.name = :name) and (:type is null or u.type  = :type ) "
			+ " order by updatetime desc")
	List<UserFileNoData> listFilesByUserid(@Param(value = "userid") int userid,
			@Param(value = "name") String name, @Param(value = "type") String type);
	
	@Query("select new net.osmand.server.api.repo.PremiumUserFilesRepository$UserFileNoData("
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
		}
	}
	
}
