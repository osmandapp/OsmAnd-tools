package net.osmand.server.api.services;

import kotlin.Pair;
import net.osmand.obf.preparation.IndexHeightData;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.PointAttributes;
import net.osmand.shared.gpx.helper.ImportGpx;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.shared.io.KFile;
import net.osmand.util.Algorithms;
import okio.Buffer;
import okio.Okio;
import okio.Source;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static net.osmand.shared.IndexConstants.GPX_FILE_EXT;

@Service
public class GpxService {
    
    protected static final Log LOGGER = LogFactory.getLog(GpxService.class);

    public static final int MAX_SIZE_FILES = 10;
    public static final int MAX_SIZE_FILES_AUTH = 100;
    public static final String ERROR_WRITING_GPX_MSG = "Error writing gpx!";
    
    @Autowired
    WebGpxParser webGpxParser;
    
    @Value("${osmand.srtm.location}")
    String srtmLocation;

    public WebGpxParser.TrackData buildTrackDataFromGpxFile(GpxFile gpxFile, GpxTrackAnalysis existingAnalysis) throws IOException {
        GpxTrackAnalysis analysis = existingAnalysis != null ? existingAnalysis : getAnalysis(gpxFile, false);

        WebGpxParser.TrackData gpxData = new WebGpxParser.TrackData();
        
        gpxData.metaData = (new WebGpxParser.WebMetaData(gpxFile.getMetadata()));
        gpxData.wpts = (webGpxParser.getWpts(gpxFile));

        Pair<List<WebGpxParser.WebTrack>, List<GpxUtilities.RouteType>> tracksResult = webGpxParser.getTracks(gpxFile);
        gpxData.tracks = (tracksResult.getFirst());
        gpxData.routeTypes = (tracksResult.getSecond());

        Map<String, String> extensions = gpxFile.getExtensions();
        gpxData.ext = (extensions);
        if (!gpxFile.getTracks().isEmpty()) {
            gpxData.trackAppearance = (new WebGpxParser.WebTrackAppearance(extensions));
        }

        if (!gpxFile.getRoutes().isEmpty()) {
            webGpxParser.addRoutePoints(gpxFile, gpxData);
        }
        gpxData.pointsGroups = (webGpxParser.getPointsGroups(gpxFile));

        addAnalysisToTrackData(gpxData, analysis);

        return gpxData;
    }

    private void addAnalysisToTrackData(WebGpxParser.TrackData gpxData, GpxTrackAnalysis gpxAnalysis) {
        if (gpxAnalysis != null) {
            gpxData.analysis = webGpxParser.getTrackAnalysis(gpxAnalysis, null);
            if (!gpxData.tracks.isEmpty() && (!gpxAnalysis.getPointAttributes().isEmpty() || (gpxAnalysis.getAvgSpeed() != 0.0 && !gpxAnalysis.hasSpeedInTrack()))) {
                boolean addSpeed = gpxAnalysis.getAvgSpeed() != 0.0 && !gpxAnalysis.hasSpeedInTrack();
                webGpxParser.addAdditionalInfo(gpxData.tracks, gpxAnalysis, addSpeed);
            }
        }
    }
    
    public WebGpxParser.TrackData addSrtmData(WebGpxParser.TrackData trackData) throws IOException {
        GpxFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        GpxTrackAnalysis srtmAnalysis = getAnalysis(gpxFile, true);
        if (srtmAnalysis != null) {
            if (trackData.analysis == null) {
            	trackData.analysis = new LinkedHashMap<>();
            }
            Map<String, Object> analysis = trackData.analysis;
            analysis.put("srtmAnalysis", true);
            analysis.put("minElevationSrtm", srtmAnalysis.getMinElevation());
            analysis.put("avgElevationSrtm", srtmAnalysis.getAvgElevation());
            analysis.put("maxElevationSrtm", srtmAnalysis.getMaxElevation());
            webGpxParser.addSrtmEle(trackData.tracks, srtmAnalysis);
            if (analysis.get("elevationData") == null) {
                webGpxParser.addAdditionalInfo(trackData.tracks, srtmAnalysis, false);
            }
        }
        return trackData;
    }
    
    public WebGpxParser.TrackData addAnalysisData(WebGpxParser.TrackData trackData) throws IOException {
        GpxFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        GpxTrackAnalysis gpxTrackAnalysis = getAnalysis(gpxFile, false);
        if (gpxTrackAnalysis != null) {
        	trackData.analysis = webGpxParser.getTrackAnalysis(gpxTrackAnalysis, null);
            boolean addSpeed = trackData.analysis.get("avgSpeed") != null && trackData.analysis.get("hasSpeedInTrack") == "false";
            if (addSpeed || trackData.analysis.get("elevationData") != null) {
                webGpxParser.addAdditionalInfo(trackData.tracks, gpxTrackAnalysis, addSpeed);
            }
        }
        return trackData;
    }
    
    private GpxTrackAnalysis getAnalysis(GpxFile gpxFile, boolean isSrtm) throws IOException {
       GpxTrackAnalysis analysis = null;
        if (!isSrtm) {
            analysis = gpxFile.getAnalysis(0);
        } else {
            GpxFile srtmGpx = calculateSrtmAltitude(gpxFile, null);
            if (srtmGpx != null && srtmGpx.getError() == null) {
                analysis = srtmGpx.getAnalysis(0);
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

    public boolean isGzipStream(InputStream in) throws IOException {
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

    public double getCommonMaxSizeFiles() {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principal instanceof WebSecurityConfiguration.OsmAndProUser) {
            return MAX_SIZE_FILES_AUTH;
        } else
            return MAX_SIZE_FILES;
    }

    public GpxFile importGpx(Source source, String filename) throws IOException {
        GpxFile gpxFile;
        if (filename.toLowerCase().endsWith(GPX_FILE_EXT)) {
            gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
        } else {
            gpxFile = ImportGpx.INSTANCE.importFile(source, filename);
        }
        return gpxFile;
    }

    public File saveMultipartFileToTemp(MultipartFile file, String httpSessionId) throws IOException {
        File tmpFile = File.createTempFile("gpx_" + httpSessionId, GPX_FILE_EXT);
        InputStream is = file.getInputStream();
        FileOutputStream fous = new FileOutputStream(tmpFile);
        Algorithms.streamCopy(is, fous);
        is.close();
        fous.close();
        return tmpFile;
    }
    
    public File createTmpFileByGpxFile(GpxFile gpxFile, String fileName) throws IOException {
        String sanitizedPrefix = fileName.replace("/../", "/");
        File tmpGpx = File.createTempFile(sanitizedPrefix, GPX_FILE_EXT);
        Exception exception = GpxUtilities.INSTANCE.writeGpxFile(new KFile(tmpGpx.getAbsolutePath()), gpxFile);
        if (exception != null) {
            throw new OsmAndPublicApiException(HttpStatus.BAD_REQUEST.value(), ERROR_WRITING_GPX_MSG);
        }
        return tmpGpx;
    }

    public GpxFile createSimplifiedGpxFile(GpxFile gpxFile) {
        GpxFile simplified = new GpxFile(null);

        simplified.setAuthor(gpxFile.getAuthor());
        simplified.setMetadata(gpxFile.getMetadata());

        List<WptPt> points = gpxFile.getPointsList();
        if (!points.isEmpty()) {
            simplified.addPoints(points);
        }

        if (!gpxFile.getTracks().isEmpty()) {
            Track track = new Track();
            simplified.getTracks().add(track);
            
            for (Track t : gpxFile.getTracks()) {
                for (TrkSegment segment : t.getSegments()) {
                    TrkSegment newSegment = new TrkSegment();
                    newSegment.getPoints().addAll(segment.getPoints());
                    track.getSegments().add(newSegment);
                }
            }
        }
        
        return simplified;
    }
}
