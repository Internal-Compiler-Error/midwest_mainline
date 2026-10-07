//! Port mapping on the home router, so peers can reach us behind NAT: NAT-PMP/PCP first
//! (one UDP round trip to the gateway, most routers made in the last decade), UPnP IGD if
//! that gets no answer. Three ports are mapped: TCP and UDP for peers (the listen port, uTP
//! shares its number) and UDP for the DHT node. Mappings are leased, renewed at half the
//! lease, and removed at shutdown.
//!
//! Best effort: no gateway, or one that refuses, is logged once and that's that. Nothing
//! else in the client knows whether a mapping exists.

use crate::events::{Event, EventBus};
use crab_nat::{GatewayAddress, InternetProtocol, PortMapping, PortMappingOptions};
use igd_next::PortMappingProtocol;
use igd_next::aio::tokio::{Tokio, search_gateway};
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU16;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// asked of the gateway; it may grant less, and renewals follow what it granted
const LEASE: Duration = Duration::from_secs(2 * 3600);
/// floor on a granted lease when scheduling renewals: a gateway granting 0 or 1 seconds
/// would otherwise have us renewing in a busy loop
const MIN_LEASE: Duration = Duration::from_secs(120);
/// how often to look again when there was no gateway to map on
const RETRY: Duration = Duration::from_secs(10 * 60);
const UPNP_SEARCH_TIMEOUT: Duration = Duration::from_secs(5);
const DESCRIPTION: &str = "downloader";

/// Where the mapping stands, for a status line.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MappingState {
    /// turned off in the settings
    Off,
    /// looking for a gateway, or asking it
    Searching,
    Mapped {
        /// what a UPnP gateway says it is; NAT-PMP doesn't say
        external_ip: Option<IpAddr>,
    },
    /// no gateway, or one that refuses; tried again every `RETRY`
    Unavailable,
}

pub(crate) type MappingWatch = watch::Receiver<MappingState>;

/// A watch that stays `Off`, for a client with mapping turned off.
pub(crate) fn none() -> MappingWatch {
    watch::channel(MappingState::Off).1
}

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

impl Protocol {
    fn igd(self) -> PortMappingProtocol {
        match self {
            Protocol::Tcp => PortMappingProtocol::TCP,
            Protocol::Udp => PortMappingProtocol::UDP,
        }
    }

    fn pmp(self) -> InternetProtocol {
        match self {
            Protocol::Tcp => InternetProtocol::Tcp,
            Protocol::Udp => InternetProtocol::Udp,
        }
    }
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
pub(crate) fn start(ports: Ports, shutdown: CancellationToken, bus: EventBus) -> MappingWatch {
    // port 0 means the OS picked one, which nobody outside can be told about; tests use it
    if ports.peer == 0 {
        return none();
    }
    let (state_tx, watch) = watch::channel(MappingState::Searching);
    let state = move |next: MappingState| {
        bus.emit(Event::PortMapping { state: next.clone() });
        let _ = state_tx.send(next);
    };
    tokio::spawn(async move {
        loop {
            state(MappingState::Searching);
            let mapper = match gateway() {
                Some((gateway, local_ip)) => {
                    let mapper = Mapper::open(gateway, local_ip, ports).await;
                    if mapper.is_none() {
                        info!("no port mapping: the gateway at {gateway} answers neither NAT-PMP nor UPnP");
                    }
                    mapper
                }
                None => {
                    debug!("no gateway to map ports on");
                    None
                }
            };
            let stopped = match mapper {
                Some(mut mapper) => {
                    state(MappingState::Mapped {
                        external_ip: mapper.external_ip().await,
                    });
                    let stopped = mapper.keep_alive(&shutdown).await;
                    mapper.close().await;
                    stopped
                }
                None => {
                    state(MappingState::Unavailable);
                    stopped_waiting(RETRY, &shutdown).await
                }
            };
            if stopped {
                return;
            }
        }
    });
    watch
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

/// Waits `d`, or until shutdown; true for the latter.
async fn stopped_waiting(d: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = sleep(d) => false,
        _ = shutdown.cancelled() => true,
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
            let Some(internal_port) = NonZeroU16::new(port) else {
                continue;
            };
            match PortMapping::new(gateway, local_ip, protocol.pmp(), internal_port, options).await {
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
        Some(mapper)
    }

    async fn external_ip(&self) -> Option<IpAddr> {
        let Mapper::Upnp { gateway, .. } = self else {
            return None;
        };
        match gateway.get_external_ip().await {
            Ok(ip) => {
                info!("UPnP gateway {} says our external address is {ip}", gateway.addr);
                Some(ip)
            }
            Err(e) => {
                debug!("UPnP gateway didn't say our external address: {e}");
                None
            }
        }
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
            let local = SocketAddr::new(*local_ip, port);
            match gateway
                .add_port(protocol.igd(), port, local, lease.as_secs() as u32, DESCRIPTION)
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

    /// Renews at half the lease until shutdown (true) or until no renewal takes (false).
    async fn keep_alive(&mut self, shutdown: &CancellationToken) -> bool {
        loop {
            let lease = match self {
                Mapper::Pmp(mappings) => mappings
                    .iter()
                    .map(|m| Duration::from_secs(m.lifetime() as u64))
                    .min()
                    .unwrap_or(LEASE),
                Mapper::Upnp { lease, .. } => *lease,
            };
            if stopped_waiting(lease.max(MIN_LEASE) / 2, shutdown).await {
                return true;
            }
            let renewed = match self {
                Mapper::Pmp(mappings) => {
                    let mut any = false;
                    for mapping in mappings.iter_mut() {
                        match mapping.renew().await {
                            Ok(()) => any = true,
                            Err(e) => warn!("renewing a port mapping failed: {e}"),
                        }
                    }
                    any
                }
                Mapper::Upnp { .. } => self.add_upnp().await,
            };
            if !renewed {
                info!("the gateway stopped renewing our port mappings; looking for one again");
                return false;
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
                    if let Err(e) = gateway.remove_port(protocol.igd(), port).await {
                        debug!("removing the UPnP mapping of {protocol} {port} failed: {e}");
                    }
                }
            }
        }
    }
}
