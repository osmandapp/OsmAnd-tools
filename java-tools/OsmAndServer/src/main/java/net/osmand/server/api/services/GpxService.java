package net.osmand.server.api.services;

import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.PointAttributes;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import okio.Buffer;
import okio.Okio;
import okio.Source;
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

import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.zip.GZIPInputStream;

@Service
public class GpxService {
    
    protected static final Log LOGGER = LogFactory.getLog(GpxService.class);
    
    @Autowired
    WebGpxParser webGpxParser;
    
    @Value("${osmand.srtm.location}")
    String srtmLocation;
    
    public WebGpxParser.TrackData getTrackDataByGpxFile(GpxFile gpxFile, File originalSourceGpx) throws IOException {
        WebGpxParser.TrackData gpxData = new WebGpxParser.TrackData();
        
        gpxData.metaData = new WebGpxParser.WebMetaData(gpxFile.getMetadata());
        gpxData.wpts = webGpxParser.getWpts(gpxFile);
        gpxData.tracks = webGpxParser.getTracks(gpxFile);
        gpxData.ext = gpxFile.getExtensions();
        
        if (!gpxFile.getRoutes().isEmpty()) {
            webGpxParser.addRoutePoints(gpxFile, gpxData);
        }
        GpxFile gpxFileForAnalyse = GpxUtilities.INSTANCE.loadGpxFile(Okio.source(originalSourceGpx));
        if (gpxFileForAnalyse.getError() == null) {
            GpxTrackAnalysis analysis = getAnalysis(gpxFileForAnalyse, false);
            gpxData.analysis = webGpxParser.getTrackAnalysis(analysis, null);
            gpxData.pointsGroups = webGpxParser.getPointsGroups(gpxFileForAnalyse);
            if (analysis != null) {
                boolean addSpeed = analysis.getAvgSpeed() != 0.0 && !analysis.hasSpeedInTrack();
                if (!gpxData.tracks.isEmpty() && (!analysis.getPointAttributes().isEmpty() || addSpeed)) {
                    webGpxParser.addAdditionalInfo(gpxData.tracks, analysis, addSpeed);
                }
            }
        }
        return gpxData;
    }
    
    public WebGpxParser.TrackData addSrtmData(WebGpxParser.TrackData trackData) throws IOException {
        GpxFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        GpxTrackAnalysis srtmAnalysis = getAnalysis(gpxFile, true);
        if (srtmAnalysis != null) {
            if (trackData.analysis == null) {
                trackData.analysis = new LinkedHashMap<>();
            }
            trackData.analysis.put("srtmAnalysis", true);
            trackData.analysis.put("minElevationSrtm", srtmAnalysis.getMinElevation());
            trackData.analysis.put("avgElevationSrtm", srtmAnalysis.getAvgElevation());
            trackData.analysis.put("maxElevationSrtm", srtmAnalysis.getMaxElevation());
            webGpxParser.addSrtmEle(trackData.tracks, srtmAnalysis);
            if (trackData.analysis.get("elevationData") == null) {
                webGpxParser.addAdditionalInfo(trackData.tracks, srtmAnalysis, false);
            }
        }
        return trackData;
    }
    
    public WebGpxParser.TrackData addAnalysisData(WebGpxParser.TrackData trackData) throws IOException {
        GpxFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        GpxTrackAnalysis analysis = getAnalysis(gpxFile, false);
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
    
    private GpxTrackAnalysis getAnalysis(GpxFile gpxFile, boolean isSrtm) throws IOException {
       GpxTrackAnalysis analysis = null;
        if (!isSrtm) {
            analysis = gpxFile.getAnalysis(System.currentTimeMillis());
        } else {
            GpxFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
            if (srtmGpx != null && srtmGpx.getError() == null) {
                analysis = srtmGpx.getAnalysis(System.currentTimeMillis());
            }
        }
        if (analysis != null) {
            cleanupFromNan(analysis);
        }
        return analysis;
    }
    
    public GpxFile calculateSrtmAltitude(GpxFile gpxFile, File[] missingFile) throws IOException {
        if (srtmLocation == null) {
            return null;
        }
        if (srtmLocation.startsWith("http://") || srtmLocation.startsWith("https://")) {
            String serverUrl = srtmLocation + "/gpx/process-srtm";
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream outputStream = new ByteArrayOutputStream();
            Exception exception = GpxUtilities.INSTANCE.writeGpx(null, Okio.buffer(Okio.sink(outputStream)), gpxFile, null);
            if (exception != null) {
                return null;
            }
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
                    if (response.getBody() == null) {
                        return null;
                    }
                    InputStream inputStream = new ByteArrayInputStream(response.getBody());
                    if (isGzipStream(inputStream)) {
                        inputStream = new GZIPInputStream(inputStream);
                    }
                    try (Source source = new Buffer().readFrom(inputStream)) {
                        return GpxUtilities.INSTANCE.loadGpxFile(source);
                    }
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
            for (Track tr : gpxFile.getTracks()) {
                for (TrkSegment s : tr.getSegments()) {
                    for (int i = 0; i < s.getPoints().size(); i++) {
                        WptPt wpt = s.getPoints().get(i);
                        double h = hd.getPointHeight(wpt.getLat(), wpt.getLon(), missingFile);
                        if (h != IndexHeightData.INEXISTENT_HEIGHT) {
                            wpt.setEle(h);
                        } else if (i == 0) {
                            return null;
                        }
                    }
                }
            }
        }
        return gpxFile;
    }

    public static boolean isGzipStream(InputStream in) throws IOException {
        in.mark(2); // mark the stream to be able to reset it
        byte[] signature = new byte[2];
        int res = in.read(signature); // read the first two bytes
        in.reset(); // reset the stream to its original state
        if (res == -1) {
            return false;
        }
        int sig = (signature[0] & 0xFF) | ((signature[1] & 0xFF) << 8);
        return sig == GZIPInputStream.GZIP_MAGIC;
    }

    
    public void cleanupFromNan(GpxTrackAnalysis analysis) {
        // process analysis
        if (Double.isNaN(analysis.getMinHdop())) {
            analysis.setMinHdop(-1);
            analysis.setMaxHdop(-1);
        }
        if (analysis.getMinSpeed() > analysis.getMaxSpeed()) {
            analysis.setMinSpeed(analysis.getMaxSpeed());
        }
        if (analysis.getStartTime() > analysis.getEndTime()) {
            analysis.setEndTime(0);
            analysis.setStartTime(0);
        }
        cleanupFromNan(analysis.getLocationStart());
        cleanupFromNan(analysis.getLocationEnd());

        Iterator<PointAttributes> iterator = analysis.getPointAttributes().iterator();
        float sumDist = 0;
        while (iterator.hasNext()) {
            PointAttributes attributes = iterator.next();
            if (Float.isNaN(attributes.getSpeed())) {
                sumDist += attributes.getDistance();
                iterator.remove();
            } else if (sumDist > 0) {
                attributes.setDistance(attributes.getDistance() + sumDist);
                sumDist = 0;
            }
        }
        iterator = analysis.getPointAttributes().iterator();
        sumDist = 0;
        while (iterator.hasNext()) {
            PointAttributes attributes = iterator.next();
            if (Float.isNaN(attributes.getElevation())) {
                sumDist += attributes.getDistance();
                iterator.remove();
            } else if (sumDist > 0) {
                attributes.setDistance(attributes.getDistance() + sumDist);
                sumDist = 0;
            }
        }
    }
    
    private void cleanupFromNan(WptPt wpt) {
        if (wpt == null) {
            return;
        }
        if (Float.isNaN(wpt.getHeading())) {
            wpt.setHeading(0);
        }
        if (Double.isNaN(wpt.getEle())) {
            wpt.setEle(99999);
        }
        if (Double.isNaN(wpt.getHdop())) {
            wpt.setHdop(-1);
        }
    }
}
