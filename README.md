# udp\_switch

`udp_switch` is a lightweight, user-space virtual switch designed to support VirtualBox VMs using UDP-encapsulated virtual NICs. It enables multiple VMs to participate in the same virtual L2 network using VirtualBox's `UDPTunnel` driver mode, without requiring physical networking hardware.

## Overview

VirtualBox's built-in UDP networking is point-to-point. This utility creates a software switch that multiplexes and replicates Ethernet frames among multiple clients (VMs or remote switches), allowing them to function as if they share a common broadcast domain.

This is useful for:

- Running VirtualBox VMs with generic UDP-based NICs in a bridged or shared segment
- Isolated L2 networks without using physical switches
- Multi-host or single-host topologies

## Features

- Supports unicast and broadcast Ethernet frame replication across UDP clients
- Simple configuration model with JSON or command-line options
- Optional VLAN-based segmentation
- Low overhead and minimal dependencies

## Build Instructions

```bash
git clone https://github.com/apschultz/udp_switch.git
cd udp_switch
make
```

This builds a single binary: `udp_switch`

## Usage

## Command-Line Options

| Option | Description |
| -------| ----------- |
| `-c <file>` or `--config <file>` | Configuration file in json format |
| `-p <port>` or `--port <port>` | Listen port (default: 10000) |
| `-b <address>` or `--bind <address>` | Listen IP address (default: ::) |
| `-C <spec>` or `--client <spec>` | Define a persistent client (repeatable)<br>Format: \<ip:port[:vlan[:mac]][@tenant_id]\><br>Note: IPv6 address must be in brackets |
| `-k` or `--keep-clients` |Keep all UDP clients after MACs have aged out for multicasts |
| `-t <seconds>` or `--timeout <seconds>` | Max age to keep MAC entries (default: 300) |
| `-m <seconds>` or `--min-age <seconds>` | Minimum age a MAC entry must be kept before moving to a new client (default: 5) |
| `-r <num>` or `--threads <num>` | Number of threads to use for processing (default: 1) |
| `-d` or `--debug` | Enable debug logging (up to 3x) |

### Define clients:

**Via command-line:**

```bash
./udp_switch -C 192.168.1.101:10001@1/08:00:27:aa:bb:01 \
             -C 192.168.1.102:10002@1/08:00:27:aa:bb:02
```

**Via JSON config:**

```json
{
  "clients": [
    {
      "addr": "192.168.1.101",
      "port": 10001,
      "vlan": 1,
      "mac": "08:00:27:aa:bb:01"
    },
    {
      "addr": "192.168.1.102",
      "port": 10002,
      "vlan": 1,
      "mac": "08:00:27:aa:bb:02"
    }
  ]
}
```

### VirtualBox VM configuration:

Configure each VM NIC to use the `UDPTunnel` driver:

```bash
VBoxManage modifyvm "vm1" --nic1 generic
VBoxManage modifyvm "vm1" --nicgenericdrv1 UDPTunnel
VBoxManage modifyvm "vm1" --nicproperty1 dest=127.0.0.1
VBoxManage modifyvm "vm1" --nicproperty1 sport=10001
VBoxManage modifyvm "vm1" --nicproperty1 dport=10000
```

Each VM will then send Ethernet frames via UDP to the switch. The switch will replicate and forward frames based on MAC and VLAN.

## Example Topology

```
+-----------+     UDP     +--------------+     UDP     +-----------+
|  VM1      |------------>| udp_switch   |<------------|  VM2      |
|  10001    |             |   port 10000 |             |  10002    |
+-----------+             +--------------+             +-----------+
```

All packets sent by VM1 are received and rebroadcast to VM2 and vice versa. Broadcast/multicast is replicated to all VLAN-matching clients.

## License

Apache 2

## Author

Adam Schultz\
[https://github.com/apschultz](https://github.com/apschultz)
