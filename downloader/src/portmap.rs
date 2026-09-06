//! Port mapping on the home router, so peers can reach us behind NAT: NAT-PMP/PCP first
//! (one UDP round trip to the gateway, most routers made in the last decade), UPnP IGD if
//! that gets no answer. Three ports are mapped: TCP and UDP for peers (the listen port, uTP
//! shares its number) and UDP for the DHT node. Mappings are leased, renewed at half the
//! lease, and removed at shutdown.
//!
//! Best effort: no gateway, or one that refuses, is logged once and that's that. Nothing
//! else in the client knows whether a mapping exists.

use crab_nat::{GatewayAddress, InternetProtocol, PortMapping, PortMappingOptions};
use igd_next::PortMappingProtocol;
use igd_next::aio::tokio::{Tokio, search_gateway};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU16;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// asked of the gateway; it may grant less, and renewals follow what it granted
const LEASE: Duration = Duration::from_secs(2 * 3600);
/// how often to look again when there was no gateway to map on
const RETRY: Duration = Duration::from_secs(10 * 60);
const UPNP_SEARCH_TIMEOUT: Duration = Duration::from_secs(5);
const DESCRIPTION: &str = "downloader";

/// Which ports to map: `peer` for TCP and UDP, `dht` (if there's a node) for UDP.
#[derive(Clone, Copy)]
pub(crate) struct Ports {
    pub peer: u16,
    pub dht: Option<u16>,
}

impl Ports {
    fn wanted(self) -> Vec<(Protocol, u16)> {
        let mut wanted = vec![(Protocol::Tcp, self.peer), (Protocol::Udp, self.peer)];
        wanted.extend(self.dht.map(|port| (Protocol::Udp, port)));
        wanted
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Protocol {
    Tcp,
    Udp,
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Protocol::Tcp => "TCP",
            Protocol::Udp => "UDP",
        })
    }
}

/// Keeps the mappings up on the current runtime until `shutdown`, then removes them.
pub(crate) fn start(ports: Ports, shutdown: CancellationToken) {
    // port 0 means the OS picked one, which nobody outside can be told about; tests use it
    if ports.peer == 0 {
        return;
    }
    tokio::spawn(async move {
        loop {
            let Some((gateway, local_ip)) = gateway() else {
                debug!("no default gateway to map ports on");
                wait_or_stop(RETRY, &shutdown).await;
                if shutdown.is_cancelled() {
                    return;
                }
                continue;
            };
            match Mapper::open(gateway, local_ip, ports).await {
                Some(mut mapper) => {
                    mapper.keep_alive(&shutdown).await;
                    mapper.close().await;
                    return;
                }
                None => {
                    info!("no port mapping: the gateway at {gateway} answers neither NAT-PMP nor UPnP");
                    wait_or_stop(RETRY, &shutdown).await;
                    if shutdown.is_cancelled() {
                        return;
                    }
                }
            }
        }
    });
}

/// A router to ask, and the address we have on its link: the default route's if it has one,
/// else any other interface's (a VPN tunnel is often the default route and has no router
/// behind it, while the LAN interface still does).
fn gateway() -> Option<(IpAddr, IpAddr)> {
    let mut interfaces = netdev::get_interfaces();
    interfaces.sort_by_key(|i| !i.default);
    interfaces.iter().find_map(|interface| {
        let gateway = interface.gateway.as_ref()?;
        if let (Some(gw), Some(local)) = (gateway.ipv4.first(), interface.ipv4.first()) {
            return Some((IpAddr::V4(*gw), IpAddr::V4(local.addr())));
        }
        if let (Some(gw), Some(local)) = (gateway.ipv6.first(), interface.ipv6.first()) {
            return Some((IpAddr::V6(*gw), IpAddr::V6(local.addr())));
        }
        None
    })
}

async fn wait_or_stop(d: Duration, shutdown: &CancellationToken) {
    tokio::select! {
        _ = sleep(d) => {}
        _ = shutdown.cancelled() => {}
    }
}

enum Mapper {
    Pmp(Vec<PortMapping>),
    Upnp {
        gateway: igd_next::aio::Gateway<Tokio>,
        local_ip: IpAddr,
        ports: Ports,
        lease: Duration,
    },
}

impl Mapper {
    async fn open(gateway_ip: IpAddr, local_ip: IpAddr, ports: Ports) -> Option<Mapper> {
        if let Some(mapper) = Self::open_pmp(gateway_ip, local_ip, ports).await {
            return Some(mapper);
        }
        Self::open_upnp(local_ip, ports).await
    }

    async fn open_pmp(gateway_ip: IpAddr, local_ip: IpAddr, ports: Ports) -> Option<Mapper> {
        let gateway = match gateway_ip {
            IpAddr::V4(ip) => GatewayAddress::IpV4(ip),
            IpAddr::V6(ip) => GatewayAddress::IpV6(ip, None),
        };
        let mut mappings = Vec::new();
        for (protocol, port) in ports.wanted() {
            let options = PortMappingOptions {
                external_port: NonZeroU16::new(port),
                lifetime_seconds: Some(LEASE.as_secs() as u32),
                timeout_config: None,
            };
            let ip_protocol = match protocol {
                Protocol::Tcp => InternetProtocol::Tcp,
                Protocol::Udp => InternetProtocol::Udp,
            };
            match PortMapping::new(gateway, local_ip, ip_protocol, NonZeroU16::new(port)?, options).await {
                Ok(mapping) => {
                    info!(
                        "mapped {protocol} {} -> {port} on the gateway with {} ({}s lease)",
                        mapping.external_port(),
                        match mapping.mapping_type() {
                            crab_nat::PortMappingType::NatPmp => "NAT-PMP",
                            crab_nat::PortMappingType::Pcp { .. } => "PCP",
                        },
                        mapping.lifetime()
                    );
                    mappings.push(mapping);
                }
                Err(e) => {
                    debug!("NAT-PMP/PCP mapping of {protocol} {port} failed: {e}");
                    // a gateway that answered once but refuses a port is still worth the
                    // rest; one that never answers isn't
                    if mappings.is_empty() {
                        return None;
                    }
                }
            }
        }
        Some(Mapper::Pmp(mappings))
    }

    async fn open_upnp(local_ip: IpAddr, ports: Ports) -> Option<Mapper> {
        let options = igd_next::SearchOptions {
            // from the LAN address, so the discovery multicast leaves on that link and not
            // through whatever holds the default route (a VPN tunnel, say)
            bind_addr: SocketAddr::new(local_ip, 0),
            timeout: Some(UPNP_SEARCH_TIMEOUT),
            ..Default::default()
        };
        let gateway = match search_gateway(options).await {
            Ok(gateway) => gateway,
            Err(e) => {
                debug!("no UPnP gateway: {e}");
                return None;
            }
        };
        let mapper = Mapper::Upnp {
            gateway,
            local_ip,
            ports,
            lease: LEASE,
        };
        if !mapper.add_upnp().await {
            return None;
        }
        if let Mapper::Upnp { gateway, .. } = &mapper
            && let Ok(ip) = gateway.get_external_ip().await
        {
            info!("UPnP gateway {} says our external address is {ip}", gateway.addr);
        }
        Some(mapper)
    }

    /// Adds (or refreshes) every UPnP mapping; true if at least one took.
    async fn add_upnp(&self) -> bool {
        let Mapper::Upnp {
            gateway,
            local_ip,
            ports,
            lease,
        } = self
        else {
            return true;
        };
        let mut any = false;
        for (protocol, port) in ports.wanted() {
            let proto = match protocol {
                Protocol::Tcp => PortMappingProtocol::TCP,
                Protocol::Udp => PortMappingProtocol::UDP,
            };
            let local = SocketAddr::new(*local_ip, port);
            match gateway
                .add_port(proto, port, local, lease.as_secs() as u32, DESCRIPTION)
                .await
            {
                Ok(()) => {
                    info!(
                        "mapped {protocol} {port} on the gateway with UPnP ({}s lease)",
                        lease.as_secs()
                    );
                    any = true;
                }
                Err(e) => warn!("UPnP mapping of {protocol} {port} failed: {e}"),
            }
        }
        any
    }

    /// Renews at half the lease until shutdown.
    async fn keep_alive(&mut self, shutdown: &CancellationToken) {
        loop {
            let lease = match self {
                Mapper::Pmp(mappings) => mappings
                    .iter()
                    .map(|m| Duration::from_secs(m.lifetime() as u64))
                    .min()
                    .unwrap_or(LEASE),
                Mapper::Upnp { lease, .. } => *lease,
            };
            wait_or_stop(lease / 2, shutdown).await;
            if shutdown.is_cancelled() {
                return;
            }
            match self {
                Mapper::Pmp(mappings) => {
                    for mapping in mappings.iter_mut() {
                        if let Err(e) = mapping.renew().await {
                            warn!("renewing a port mapping failed: {e}");
                        }
                    }
                }
                Mapper::Upnp { .. } => {
                    self.add_upnp().await;
                }
            }
        }
    }

    async fn close(self) {
        match self {
            Mapper::Pmp(mappings) => {
                for mapping in mappings {
                    if let Err((e, _)) = mapping.try_drop().await {
                        debug!("removing a port mapping failed: {e}");
                    }
                }
            }
            Mapper::Upnp { gateway, ports, .. } => {
                for (protocol, port) in ports.wanted() {
                    let proto = match protocol {
                        Protocol::Tcp => PortMappingProtocol::TCP,
                        Protocol::Udp => PortMappingProtocol::UDP,
                    };
                    if let Err(e) = gateway.remove_port(proto, port).await {
                        debug!("removing the UPnP mapping of {protocol} {port} failed: {e}");
                    }
                }
            }
        }
    }
}
