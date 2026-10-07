package net.osmand.server.api.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.cache.Cache;

import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;

@RunWith(Parameterized.class)
public class UserdataServiceEmailTokenTest {

    @Parameterized.Parameters(name = "{0} cached users -> {1} minutes, {2} digits")
    public static Object[][] usersDelayAndDigits() {
        // Users already in the cache before issuing the next user's token.
        return new Object[][] {
                {0, 10, 6},
                {50, 10, 6},
                {100, 10, 6},
                {109, 10, 6},
                {110, 11, 6},
                {116, 11, 6},
                {117, 11, 7},
                {134, 13, 8},
                {150, 15, 9},
                {167, 16, 10},
                {184, 18, 11},
                {199, 19, 11},
                {200, 20, 12},
                {300, 30, 12},
                {400, 40, 12},
                {500, 50, 12},
                {599, 59, 12},
                {600, 60, 12},
                {1000, 60, 12}
        };
    }

    @Parameterized.Parameter
    public int cachedUsers;

    @Parameterized.Parameter(1)
    public int expectedDelayMinutes;

    @Parameterized.Parameter(2)
    public int expectedDigits;

    @Test
    public void delayAndDigitsDependOnUserCountAndResendPreservesToken() {
        UserdataService service = new UserdataService();
        Cache<?, ?> cache = (Cache<?, ?>) ReflectionTestUtils.getField(service, "emailTokenRequests");
        for (int i = 0; i < cachedUsers; i++) {
            service.updateSecureEmailToken(user("user" + i + "@example.com"));
        }
        assertEquals(cachedUsers, cache.size());

        CloudUser user = user("next@example.com");
        service.updateSecureEmailToken(user);
        Object cachedToken = cache.getIfPresent(user.email);
        long nextAllowedAt = (long) ReflectionTestUtils.getField(cachedToken, "nextAllowedAt");
        assertEquals(TimeUnit.MINUTES.toMillis(expectedDelayMinutes), nextAllowedAt - user.tokenTime.getTime());
        assertEquals(expectedDigits, user.token.length());
        assertTrue(user.token.matches("[1-9][0-9]*"));

        String token = user.token;
        Date tokenTime = user.tokenTime;
        service.updateSecureEmailToken(user);
        assertEquals(token, user.token);
        assertSame(tokenTime, user.tokenTime);
        assertSame(cachedToken, cache.getIfPresent(user.email));
        assertEquals(cachedUsers + 1, cache.size());
    }

    private static CloudUser user(String email) {
        CloudUser user = new CloudUser();
        user.email = email;
        return user;
    }
}
