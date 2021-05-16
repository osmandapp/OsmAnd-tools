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

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;

@Repository
public interface PremiumUserFilesRepository extends JpaRepository<UserFile, Long> {
	
	UserFile findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(int userid, String name, String type);
	
	UserFile findTopByUseridAndNameAndTypeAndUpdatetime(int userid, String name, String type, Date updatetime);
	
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
        public int filesize;
        
        @Column(name = "updatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date updatetime;
        
        @Column(name = "clienttime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date clienttime;
        
        @Column(name = "storage")
        public String storage;
        
//      @Fetch(FetchMode.JOIN)
        @Column(name = "data", columnDefinition="bytea")
        public byte[] data;
//        @Lob
//        public Blob data;

    }

    
    // COALESCE(length(u.data), -1))
	@Query("select new net.osmand.server.api.repo.PremiumUserFilesRepository$UserFileNoData(u.id, u.userid, u.deviceid, u.type, u.name, u.updatetime, u.clienttime, u.filesize, length(u.data) ) "
			+ " from UserFile u "
			+ " where u.userid = :userid  and (:name is null or u.name = :name) and (:type is null or u.type  = :type ) "
			+ " order by updatetime desc")
	List<UserFileNoData> listFilesByUserid(@Param(value = "userid") int userid, @Param(value = "name") String name, @Param(value = "type") String type);
	
	// file used to be transmitted to client as is
	class UserFileNoData {
		public int userid;
		public long id;
        public int deviceid;
        public int filesize;
        public String type;
        public String name;
        public Date updatetime;
        public long updatetimems;
        public Date clienttime;
        public long clienttimems;
		public int zipSize;
		
		public UserFileNoData(UserFile c) {
			this.userid = c.userid;
			this.id = c.id;
			this.deviceid = c.deviceid;
			this.type = c.type;
			this.name = c.name;
			this.filesize = c.filesize ;
			this.zipSize = c.data == null ? 0 : c.data.length;
			this.updatetime = c.updatetime;
			this.updatetimems = updatetime == null ? 0 : updatetime.getTime();
			this.clienttime = c.clienttime;
			this.clienttimems = clienttime == null? 0 : clienttime.getTime();
		}
		
		
		public UserFileNoData(long id, int userid, int deviceid, String type, String name, Date updatetime, Date clienttime, Integer filesize, Integer zipSize) {
			this.userid = userid;
			this.id = id;
			this.deviceid = deviceid;
			this.type = type;
			this.name = name;
			this.filesize = filesize == null ? 0 : filesize.intValue();
			this.zipSize = zipSize == null ? 0 : zipSize.intValue();
			this.updatetime = updatetime;
			this.updatetimems = updatetime == null ? 0 : updatetime.getTime();
			this.clienttime = clienttime;
			this.clienttimems = clienttime == null? 0 : clienttime.getTime();
		}
	}
	

}
