package io.github.dk900912.multitiercache.provider.lettuce;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LettuceL2ProviderConfigurationTest {

    @Test
    void parsesIpv4HostnamesAndIpv6Endpoints() {
        assertEquals(new LettuceL2Provider.Endpoint("localhost", 6379),
                LettuceL2Provider.Endpoint.parse("localhost"));
        assertEquals(new LettuceL2Provider.Endpoint("127.0.0.1", 7001),
                LettuceL2Provider.Endpoint.parse("127.0.0.1:7001"));
        assertEquals(new LettuceL2Provider.Endpoint("2001:db8::1", 6379),
                LettuceL2Provider.Endpoint.parse("2001:db8::1"));
        assertEquals(new LettuceL2Provider.Endpoint("2001:db8::1", 7001),
                LettuceL2Provider.Endpoint.parse("[2001:db8::1]:7001"));
        assertEquals(new LettuceL2Provider.Endpoint("2001:db8::1", 6379),
                LettuceL2Provider.Endpoint.parse("[2001:db8::1]"));

        assertThrows(IllegalArgumentException.class, () -> LettuceL2Provider.Endpoint.parse("localhost:0"));
        assertThrows(IllegalArgumentException.class, () -> LettuceL2Provider.Endpoint.parse("localhost:65536"));
        assertThrows(IllegalArgumentException.class, () -> LettuceL2Provider.Endpoint.parse("[2001:db8::1"));
    }
}
