use std::net::IpAddr;

use http::HeaderMap;

use super::RateLimiter;

impl RateLimiter {
    pub(crate) fn anonymous_client_ip(
        &self,
        peer: IpAddr,
        headers: &HeaderMap,
        get_object: bool,
    ) -> IpAddr {
        let is_trusted = |ip: IpAddr| {
            // Dual-stack listeners may report IPv4 peers as ::ffff:a.b.c.d.
            let canonical = ip.to_canonical();
            self.config
                .trusted_proxies
                .iter()
                .any(|net| net.contains(&ip) || (ip != canonical && net.contains(&canonical)))
        };
        if !self.config.anonymous.applies_to(get_object)
            || !(self.config.trust_proxy_headers || is_trusted(peer))
        {
            return peer;
        }

        let forwarded = headers.get_all("x-forwarded-for");
        let mut lines = forwarded.iter().rev().peekable();
        if lines.peek().is_some() {
            let mut client = peer;
            // Bytes to the left of the first untrusted hop are outside the trust boundary.
            for line in lines {
                for hop in line.as_bytes().rsplit(|&byte| byte == b',') {
                    let Some(ip) = parse_ip(hop) else {
                        return peer;
                    };
                    if !is_trusted(ip) {
                        return ip;
                    }
                    client = ip;
                }
            }
            return client;
        }

        let real_ip = headers.get_all("x-real-ip");
        let mut values = real_ip.iter();
        match (values.next(), values.next()) {
            (Some(value), None) => parse_ip(value.as_bytes()).unwrap_or(peer),
            _ => peer,
        }
    }
}

fn parse_ip(bytes: &[u8]) -> Option<IpAddr> {
    std::str::from_utf8(bytes)
        .ok()?
        .trim_matches([' ', '\t'])
        .parse()
        .ok()
}
