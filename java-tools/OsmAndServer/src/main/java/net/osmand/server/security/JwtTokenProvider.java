package net.osmand.server.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.util.Date;

@Component
public class JwtTokenProvider {
    
    private static final String TOKEN_TYPE_ROOM = "ROOM";
    private static final String ANONYMOUS_ALIAS = "anonymous";
    
    private final SecretKey key;
    
    public JwtTokenProvider(@Value("${jwt.secret:default-secret-key-min-256-bits-for-hs256-algorithm}") String secret) {
        // Minimum 256 bits for HS256
        if (secret.length() < 32) {
            throw new IllegalArgumentException("JWT secret must be at least 32 characters (256 bits)");
        }
        this.key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Creates a ROOM token for WebSocket room access.
     * 
     * @param translationId translation/room ID
     * @param alias user alias
     * @param validityMs token validity in milliseconds
     * @return JWT token string
     */
    public String createRoomToken(String translationId, String alias, long validityMs) {
        Date now = new Date();
        Date validity = new Date(now.getTime() + validityMs);
        
        return Jwts.builder()
            .subject(alias != null ? alias : ANONYMOUS_ALIAS)
            .claim("type", TOKEN_TYPE_ROOM)
            .claim("translationId", translationId)
            .issuedAt(now)
            .expiration(validity)
            .signWith(key)
            .compact();
    }
    
    /**
     * Checks if token is a ROOM token.
     * 
     * @param token JWT token string
     * @return true if token is a valid ROOM token
     */
    public boolean isRoomToken(String token) {
        try {
            Claims claims = validateToken(token);
            return TOKEN_TYPE_ROOM.equals(claims.get("type", String.class));
        } catch (JwtException e) {
            return false;
        }
    }

    /**
     * Validates JWT token and returns claims.
     *
     * @param token JWT token string
     * @return Claims if token is valid
     * @throws JwtException if token is invalid or expired
     */
    public Claims validateToken(String token) throws JwtException {
        return Jwts.parser()
                .verifyWith(key)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
}
