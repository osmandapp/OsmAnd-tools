package net.osmand.server.api.services;

import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.gpx.GPXTrackAnalysis;
import net.osmand.gpx.PointAttributes;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.server.utils.WebGpxParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;

@Service
public class GpxService {
    
    protected static final Log LOGGER = LogFactory.getLog(GpxService.class);
    
    @Autowired
    WebGpxParser webGpxParser;
    
    @Value("${osmand.srtm.location}")
    String srtmLocation;
    
    public WebGpxParser.TrackData getTrackDataByGpxFile(GPXFile gpxFile, File originalSourceGpx) {
        WebGpxParser.TrackData gpxData = new WebGpxParser.TrackData();
        
        gpxData.metaData = new WebGpxParser.MetaData(gpxFile.metadata);
        gpxData.wpts = webGpxParser.getWpts(gpxFile);
        gpxData.tracks = webGpxParser.getTracks(gpxFile);
        gpxData.ext = gpxFile.extensions;
        
        if (!gpxFile.routes.isEmpty()) {
            webGpxParser.addRoutePoints(gpxFile, gpxData);
        }
        GPXFile gpxFileForAnalyse = GPXUtilities.loadGPXFile(originalSourceGpx);
        GPXTrackAnalysis analysis = getAnalysis(gpxFileForAnalyse, false);
        gpxData.analysis = webGpxParser.getTrackAnalysis(analysis, null);
        gpxData.pointsGroups = webGpxParser.getPointsGroups(gpxFileForAnalyse);
        if (analysis != null) {
            boolean addSpeed = analysis.getAvgSpeed() != 0.0 && !analysis.hasSpeedInTrack();
            if (!gpxData.tracks.isEmpty() && (!analysis.pointAttributes.isEmpty() || addSpeed)) {
                webGpxParser.addAdditionalInfo(gpxData.tracks, analysis, addSpeed);
            }
        }
        return gpxData;
    }
    
    public WebGpxParser.TrackData addSrtmData(WebGpxParser.TrackData trackData) {
        GPXFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        GPXTrackAnalysis srtmAnalysis = getAnalysis(gpxFile, true);
        if (srtmAnalysis != null) {
            if (trackData.analysis == null) {
                trackData.analysis = new LinkedHashMap<>();
            }
            trackData.analysis.put("srtmAnalysis", true);
            trackData.analysis.put("minElevationSrtm", srtmAnalysis.minElevation);
            trackData.analysis.put("avgElevationSrtm", srtmAnalysis.avgElevation);
            trackData.analysis.put("maxElevationSrtm", srtmAnalysis.maxElevation);
            webGpxParser.addSrtmEle(trackData.tracks, srtmAnalysis);
            if (trackData.analysis.get("elevationData") == null) {
                webGpxParser.addAdditionalInfo(trackData.tracks, srtmAnalysis, false);
            }
        }
        return trackData;
    }
    
    public WebGpxParser.TrackData addAnalysisData(WebGpxParser.TrackData trackData) {
        GPXFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        GPXTrackAnalysis analysis = getAnalysis(gpxFile, false);
        if (analysis != null) {
            if (trackData.analysis == null) {
                trackData.analysis = new LinkedHashMap<>();
            }
            trackData.analysis = webGpxParser.getTrackAnalysis(analysis, null);
            boolean addSpeed = trackData.analysis.get("avgSpeed") != null && trackData.analysis.get("hasSpeedInTrack") == "false";
            if (addSpeed || trackData.analysis.get("elevationData") != null) {
                webGpxParser.addAdditionalInfo(trackData.tracks, analysis, addSpeed);
            }
        }
        return trackData;
    }
    
    private GPXTrackAnalysis getAnalysis(GPXFile gpxFile, boolean isSrtm) {
       GPXTrackAnalysis analysis = null;
        if (!isSrtm) {
            analysis = gpxFile.getAnalysis(System.currentTimeMillis());
        } else {
            GPXFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
            if (srtmGpx != null) {
                analysis = srtmGpx.getAnalysis(System.currentTimeMillis());
            }
        }
        if (analysis != null) {
            cleanupFromNan(analysis);
        }
        return analysis;
    }
    
    public GPXFile calculateSrtmAltitude(GPXFile gpxFile, File[] missingFile) {
        if (srtmLocation == null) {
            return null;
        }
        if (srtmLocation.startsWith("http://") || srtmLocation.startsWith("https://")) {
            String serverUrl = srtmLocation + "/gpx/process-srtm";
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GPXUtilities.writeGpx(new OutputStreamWriter(baos), gpxFile, null);
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);
            
            MultiValueMap<String, String> fileMap = new LinkedMultiValueMap<>();
            ContentDisposition contentDisposition = ContentDisposition.builder("form-data").name("file")
                    .filename("route.gpx").build();
            fileMap.add(HttpHeaders.CONTENT_DISPOSITION, contentDisposition.toString());
            HttpEntity<byte[]> fileEntity = new HttpEntity<>(baos.toByteArray(), fileMap);
            
            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            body.add("file", fileEntity);
            
            HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);
            RestTemplate restTemplate = new RestTemplate();
            try {
                ResponseEntity<byte[]> response = restTemplate.postForEntity(serverUrl, requestEntity, byte[].class);
                if (response.getStatusCode().is2xxSuccessful()) {
                    return GPXUtilities.loadGPXFile(new ByteArrayInputStream(response.getBody()));
                }
            } catch (RestClientException e) {
                LOGGER.error(e.getMessage(), e);
            }
            return null;
        } else {
            File srtmFolder = new File(srtmLocation);
            if (!srtmFolder.exists()) {
                return null;
            }
            IndexHeightData hd = new IndexHeightData();
            hd.setSrtmData(srtmFolder.getAbsolutePath(), srtmFolder);
            for (GPXUtilities.Track tr : gpxFile.tracks) {
                for (GPXUtilities.TrkSegment s : tr.segments) {
                    for (int i = 0; i < s.points.size(); i++) {
                        GPXUtilities.WptPt wpt = s.points.get(i);
                        double h = hd.getPointHeight(wpt.lat, wpt.lon, missingFile);
                        if (h != IndexHeightData.INEXISTENT_HEIGHT) {
                            wpt.ele = h;
                        } else if (i == 0) {
                            return null;
                        }
                    }
                }
            }
        }
        return gpxFile;
    }
    
    public void cleanupFromNan(GPXTrackAnalysis analysis) {
        // process analysis
        if (Double.isNaN(analysis.minHdop)) {
            analysis.minHdop = -1;
            analysis.maxHdop = -1;
        }
        if (analysis.getMinSpeed() > analysis.getMaxSpeed()) {
            analysis.setMinSpeed(analysis.getMaxSpeed());
        }
        if (analysis.getStartTime() > analysis.getEndTime()) {
            analysis.setEndTime(0);
            analysis.setStartTime(0);
        }
        cleanupFromNan(analysis.locationStart);
        cleanupFromNan(analysis.locationEnd);

        Iterator<PointAttributes> iterator = analysis.pointAttributes.iterator();
        float sumDist = 0;
        while (iterator.hasNext()) {
            PointAttributes attributes = iterator.next();
            if (Float.isNaN(attributes.speed)) {
                sumDist += attributes.distance;
                iterator.remove();
            } else if (sumDist > 0) {
                attributes.distance += sumDist;
                sumDist = 0;
            }
        }
        iterator = analysis.pointAttributes.iterator();
        sumDist = 0;
        while (iterator.hasNext()) {
            PointAttributes attributes = iterator.next();
            if (Float.isNaN(attributes.elevation)) {
                sumDist += attributes.distance;
                iterator.remove();
            } else if (sumDist > 0) {
                attributes.distance += sumDist;
                sumDist = 0;
            }
        }
    }
    
    private void cleanupFromNan(GPXUtilities.WptPt wpt) {
        if (wpt == null) {
            return;
        }
        if (Float.isNaN(wpt.heading)) {
            wpt.heading = 0;
        }
        if (Double.isNaN(wpt.ele)) {
            wpt.ele = 99999;
        }
        if (Double.isNaN(wpt.hdop)) {
            wpt.hdop = -1;
        }
    }
}
