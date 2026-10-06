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

    @Parameterized.Parameters(name = "{0} cached users -> {1} minutes")
    public static Object[][] usersAndDelay() {
        // Users already in the cache before issuing the next user's token.
        return new Object[][] {
                {0, 10},
                {50, 10},
                {100, 10},
                {109, 10},
                {110, 11},
                {150, 15},
                {200, 20},
                {300, 30},
                {400, 40},
                {500, 50},
                {599, 59},
                {600, 60},
                {1000, 60}
        };
    }

    @Parameterized.Parameter
    public int cachedUsers;

    @Parameterized.Parameter(1)
    public int expectedDelayMinutes;

    @Test
    public void delayDependsOnUserCountAndResendPreservesToken() {
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
        assertTrue(user.token.matches("[1-9][0-9]{5}"));

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
