package net.osmand.server.controllers.pub;

import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.server.api.repo.OsmRecipientsRepository;
import net.osmand.server.api.repo.OsmRecipientsRepository.OsmRecipient;
import net.osmand.server.api.repo.SupporterSubscriptionRepository;
import net.osmand.server.api.repo.SupportersRepository;
import net.osmand.server.api.repo.SupportersRepository.Supporter;
import net.osmand.server.api.repo.SupporterSubscriptionRepository.SupporterSubscription;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/subscription")
public class SubscriptionController {
    private static final Log LOGGER = LogFactory.getLog(SubscriptionController.class);

    private static final String ERROR_MESSAGE_TEMPLATE = "{\"error\": \"%s is not specified\"}";

    @Autowired
    private SupportersRepository supportersRepository;
    @Autowired
    private SupporterSubscriptionRepository supporterSubscriptionRepository;
    @Autowired
    private MapUserRepository mapUserRepository;
    @Autowired
    private OsmRecipientsRepository osmRecipientsRepository;

    private final RestTemplate restTemplate;

    public SubscriptionController(RestTemplateBuilder builder) {
        this.restTemplate = builder.requestFactory(HttpComponentsClientHttpRequestFactory.class).build();
    }

    private void checkParameter(String paramName, String paramValue) {
        if (paramValue.isEmpty()) {
            throw new MissingRequestParameterException(paramName);
        }
    }

    private void validateBitcoinAddress(String bitcoinAddress) {
        if (!BTCAddrValidator.validate(bitcoinAddress)) {
            throw new BitcoinAddressInvalidException("Address is invalid");
        }
    }

    private String encodeCredentialsToBase64(String userName, String password) {
        Base64.Encoder encoder = Base64.getMimeEncoder();
        byte[] credentials = userName.concat(":").concat(password).getBytes();
        return encoder.encodeToString(credentials);
    }

    private String processOsmUsername(String userName) {
        int ind = userName.indexOf('\'');
        if (ind > -1) {
            return userName.substring(0, ind).substring(ind + 1);
        }
        return userName;
    }

    private HttpHeaders buildHeaders(String credentials) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.set(HttpHeaders.AUTHORIZATION, String.format("Basic %s", credentials));
        return headers;
    }

    private void authenticateUser(String credentials) {
        HttpHeaders headers = buildHeaders(credentials);
        makeRequest(headers);
    }

    private void makeRequest(HttpHeaders headers) {
        HttpEntity<String> requestEntity = new HttpEntity<>(null, headers);
        restTemplate.exchange("https://api.openstreetmap.org/api/0.6/user/details",
                HttpMethod.GET, requestEntity, String.class);
    }

    @PostMapping(path = {"/register_email", "/register_email.php"},
            consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<MapUser> registerEmail(@RequestParam("aid") String aid,
                                                                   @RequestParam("email") String email) {
        checkParameter("aid", aid);
        checkParameter("E-mail", email);
        long timestamp = System.currentTimeMillis();
        MapUser mapUser = new MapUser();
        mapUser.setAid(aid);
        mapUser.setEmail(email);
        mapUser.setUpdateTime(timestamp);
        mapUser = mapUserRepository.save(mapUser);
        return ResponseEntity.ok(mapUser);
    }

    @PostMapping(path = {"/register", "/register.php"},
        consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
        produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Supporter> register(@RequestParam("visibleName") String visibleName,
                                              @RequestParam("email") String email,
                                              @RequestParam("preferredCountry") String preferredCountry) {
        checkParameter("Visible Name", visibleName);
        checkParameter("E-mail", email);
        checkParameter("Preferred Country", preferredCountry);
        Optional<Supporter> optionalSupporter = supportersRepository.findByUserEmail(email);
        if (optionalSupporter.isPresent()) {
            Supporter supporter = optionalSupporter.get();
            return ResponseEntity.ok(supporter);
        }
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        int token = tlr.nextInt(100000, 1000000);
        Supporter supporter = new Supporter(0L , String.valueOf(token),
                visibleName, email, preferredCountry, 0);
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(supporter);
    }

    @PostMapping(path = {"/update", "/update.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Supporter> update(@RequestParam("visibleName") String visibleName,
                                            @RequestParam("email") String email,
                                            @RequestParam("token") String token,
                                            @RequestParam("preferredCountry") String preferredCountry,
                                            @RequestParam("userid") Long userid) {
        checkParameter("Visible Name", visibleName);
        checkParameter("E-mail", email);
        checkParameter("Token", token);
        checkParameter("Preferred Country", preferredCountry);
        Supporter supporter = new Supporter(userid, token, visibleName, email, preferredCountry, 0);
        supporter = supportersRepository.saveAndFlush(supporter);
        return ResponseEntity.ok(supporter);
    }

    @PostMapping(path = {"/register_osm", "/register_osm.php"},
            consumes =  MediaType.APPLICATION_FORM_URLENCODED_VALUE,
            produces =  MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<OsmRecipient> registerOsm(@RequestParam("bitcoin_addr") String bitcoinAddress,
                                                    @RequestParam("osm_usr") String osmUser,
                                                    @RequestParam("osm_pwd") String osmPassword,
                                                    @RequestParam("email") String email) {
        checkParameter("Bitcoin address", bitcoinAddress);
        validateBitcoinAddress(bitcoinAddress);
        checkParameter("E-mail", email);
        checkParameter("Osm user", osmUser);
        checkParameter("Osm password", osmPassword);
        String username = processOsmUsername(osmUser);
        String credentials = encodeCredentialsToBase64(username, osmPassword);
        authenticateUser(credentials);
        long registerTimestamp = System.currentTimeMillis();
        OsmRecipient recipient = new OsmRecipient(osmUser, email, bitcoinAddress, registerTimestamp);
        recipient = osmRecipientsRepository.save(recipient);
        return ResponseEntity.ok(recipient);
    }

    @PostMapping(path = {"/purchased", "/purchased.php"})
    public ResponseEntity<Supporter> purchased(@RequestParam("userid") Long userId,
                                            @RequestParam("sku") String sku,
                                            @RequestParam("purchaseToken") String purchaseToken) {
        checkParameter("sku", sku);
        checkParameter("purchase token", purchaseToken);
        Optional<Supporter> supporterOptional = supportersRepository.findById(userId);
        if (!supporterOptional.isPresent()) {
            throw new SupporterSubscriptionNotFoundException("User not found with given id : " + userId);
        }
        long checkTime = System.currentTimeMillis();
        SupporterSubscription subscription = new SupporterSubscription();
        subscription.setUserId(userId);
        subscription.setSku(sku);
        subscription.setPurchaseToken(purchaseToken);
        subscription.setCheckTime(checkTime);
        supporterSubscriptionRepository.save(subscription);
        return ResponseEntity.ok(supporterOptional.get());
    }

    @ExceptionHandler(MissingRequestParameterException.class)
    public ResponseEntity<String> missingParameterHandler(MissingRequestParameterException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.badRequest().body(String.format(ERROR_MESSAGE_TEMPLATE, ex.getMessage()));
    }

    @ExceptionHandler(BitcoinAddressInvalidException.class)
    public ResponseEntity<String> bitcoinAddressInvalidHandler(BitcoinAddressInvalidException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.badRequest().body("{\"error\": \"%s\"}");
    }

    @ExceptionHandler(SupporterSubscriptionNotFoundException.class)
    public ResponseEntity<String> supporterSubscriptionNotFoundHandler(SupporterSubscriptionNotFoundException ex) {
        LOGGER.error(ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(
                "{\"error\": \"Supporter subscription not found. Check user id\"}");
    }

    private static class MissingRequestParameterException extends RuntimeException {
        MissingRequestParameterException(String s) {
            super(s);
        }
    }

    private static class BitcoinAddressInvalidException extends RuntimeException {
        BitcoinAddressInvalidException(String s) {
            super(s);
        }
    }

    private static class SupporterSubscriptionNotFoundException extends RuntimeException {
        SupporterSubscriptionNotFoundException(String s) {
            super(s);
        }
    }


    private static class BTCAddrValidator {

        private static final char[] ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".toCharArray();

        private static final int[] INDEXES = new int[128];

        private static final MessageDigest digest;

        static {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            for (int i = 0; i < INDEXES.length; i++) {
                INDEXES[i] = -1;
            }
            for (int i = 0; i < ALPHABET.length; i++) {
                INDEXES[ALPHABET[i]] = i;
            }
        }

        public static boolean validate(String addr) {
            try {
                int addressHeader = getAddressHeader(addr);
                return (addressHeader == 0 || addressHeader == 5);
            } catch (Exception x) {
                x.printStackTrace();
            }
            return false;
        }

        private static int getAddressHeader(String address) throws IOException {
            byte[] tmp = decodeChecked(address);
            return tmp[0] & 0xFF;
        }

        private static byte[] decodeChecked(String input) throws IOException {
            byte[] tmp = decode(input);
            if (tmp.length < 4)
                throw new IOException("BTC AddressFormatException Input too short");
            byte[] bytes = copyOfRange(tmp, 0, tmp.length - 4);
            byte[] checksum = copyOfRange(tmp, tmp.length - 4, tmp.length);

            tmp = doubleDigest(bytes);
            byte[] hash = copyOfRange(tmp, 0, 4);
            if (!Arrays.equals(checksum, hash))
                throw new IOException("BTC AddressFormatException Checksum does not validate");

            return bytes;
        }

        private static byte[] doubleDigest(byte[] input) {
            return doubleDigest(input, 0, input.length);
        }

        private static byte[] doubleDigest(byte[] input, int offset, int length) {
            synchronized (digest) {
                digest.reset();
                digest.update(input, offset, length);
                byte[] first = digest.digest();
                return digest.digest(first);
            }
        }

        private static byte[] decode(String input) throws IOException {
            if (input.length() == 0) {
                return new byte[0];
            }
            byte[] input58 = new byte[input.length()];
            // Transform the String to a base58 byte sequence
            for (int i = 0; i < input.length(); ++i) {
                char c = input.charAt(i);
                int digit58 = -1;
                if (c >= 0 && c < 128) {
                    digit58 = INDEXES[c];
                }
                if (digit58 < 0) {
                    throw new IOException("Bitcoin AddressFormatException Illegal character " + c + " at " + i);
                }

                input58[i] = (byte) digit58;
            }

            // Count leading zeroes
            int zeroCount = 0;
            while (zeroCount < input58.length && input58[zeroCount] == 0) {
                ++zeroCount;
            }
            // The encoding
            byte[] temp = new byte[input.length()];
            int j = temp.length;

            int startAt = zeroCount;
            while (startAt < input58.length) {
                byte mod = divmod256(input58, startAt);
                if (input58[startAt] == 0) {
                    ++startAt;
                }

                temp[--j] = mod;
            }
            // Do no add extra leading zeroes, move j to first non null byte.
            while (j < temp.length && temp[j] == 0) {
                ++j;
            }

            return copyOfRange(temp, j - zeroCount, temp.length);
        }

        private static byte divmod256(byte[] number58, int startAt) {
            int remainder = 0;
            for (int i = startAt; i < number58.length; i++) {
                int digit58 = (int) number58[i] & 0xFF;
                int temp = remainder * 58 + digit58;

                number58[i] = (byte) (temp / 256);

                remainder = temp % 256;
            }
            return (byte) remainder;
        }

        private static byte[] copyOfRange(byte[] source, int from, int to) {
            byte[] range = new byte[to - from];
            System.arraycopy(source, from, range, 0, range.length);
            return range;
        }
    }
}
