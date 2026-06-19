package io.github.dk900912.multitiercache.provider.redisson;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RedissonL2ProviderConfigurationTest {

    @Test
    void normalizesIpv4HostnamesAndIpv6Endpoints() {
        assertEquals("redis://localhost:6379", RedissonL2Provider.normalizeAddress("localhost"));
        assertEquals("redis://127.0.0.1:7001", RedissonL2Provider.normalizeAddress("127.0.0.1:7001"));
        assertEquals("redis://[2001:db8::1]:6379", RedissonL2Provider.normalizeAddress("2001:db8::1"));
        assertEquals("redis://[2001:db8::1]:7001", RedissonL2Provider.normalizeAddress("[2001:db8::1]:7001"));
        assertEquals("redis://[2001:db8::1]:6379", RedissonL2Provider.normalizeAddress("[2001:db8::1]"));
        assertEquals("rediss://[2001:db8::1]:7001",
                RedissonL2Provider.normalizeAddress("rediss://[2001:db8::1]:7001"));

        assertThrows(IllegalArgumentException.class, () -> RedissonL2Provider.normalizeAddress("localhost:0"));
        assertThrows(IllegalArgumentException.class, () -> RedissonL2Provider.normalizeAddress("localhost:65536"));
        assertThrows(IllegalArgumentException.class, () -> RedissonL2Provider.normalizeAddress("redis://2001:db8::1"));
    }
}
