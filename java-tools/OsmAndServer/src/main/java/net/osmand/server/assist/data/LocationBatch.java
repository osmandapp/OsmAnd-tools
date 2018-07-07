package net.osmand.server.assist.data;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Type;

import com.google.gson.JsonObject;



public class LocationBatch {

    @Column(name="DATE_CREATED")
    @Temporal(TemporalType.TIMESTAMP)
    public java.util.Date dateCreated;
	
    public Long idToStore;
    
    
    public double lat;
    
    public double lon;
    
    public int size;
    
    @Column(name = "data", columnDefinition = "jsonb")
    @Type(type = "net.osmand.server.assist.JsonbType") 
	public JsonObject deltaEncodedLocations;
	
}
