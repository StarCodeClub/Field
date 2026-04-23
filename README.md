# Field (场)

A Velocity 3.4.0 proxy plugin that provides TCP-level connection control. Unlike conventional ban plugins that send disconnect packets, Field operates at the Netty pipeline layer to reject connections before any Minecraft protocol data is exchanged — making the server appear completely offline to blocked clients.  
`一个适用于 Velocity 3.4.0 的代理端插件，可提供 TCP 级别的连接控制。与发送断开连接数据包的传统封禁插件不同，本插件在 Netty 管道层运行，能在任何 Minecraft 协议进行数据交换之前拒绝连接 ——— 使服务器在被阻止的客户端中展示出逼真的离线状态。`

## How It Works

### Netty Pipeline Injection

Velocity uses Netty as its networking layer. When a TCP connection arrives, it flows through a pipeline of handlers: accept → initialize → decode → handle. Field injects itself at two critical points:

**1. ChannelInitializer Wrapping**

Velocity's `ConnectionManager` holds a `ServerChannelInitializerHolder`, which contains the `ChannelInitializer<Channel>` applied to every new inbound connection. During plugin initialization, Field uses reflection to:

- Locate the `ConnectionManager` via the `cm` field on `VelocityServer`
- Extract the `ServerChannelInitializerHolder`
- Call `holder.get()` to obtain the original `ChannelInitializer`
- Replace it with a wrapper that checks bans/vanish/rate-limits **before** calling the original

This means blocked connections never reach Velocity's handshake decoder. The TCP socket is opened and immediately closed — no Minecraft packets are sent in either direction.

**2. Boss Pipeline Handler**

After the server socket binds (detected via a delayed scan of the `endpoints` Multimap in `ConnectionManager`), Field injects a `ChannelInboundHandlerAdapter` named `field-acceptor` at the **head** of the `ServerChannel` pipeline. This handler intercepts the `channelRead` event that delivers newly accepted child channels.

At this stage, child channels are not yet registered to a worker EventLoop. Calling `channel.close()` would throw `IllegalStateException`. Instead, Field calls `channel.unsafe().closeForcibly()`, which directly closes the underlying `java.nio.channels.SocketChannel` without requiring EventLoop registration.

### Reflection Strategy

Field depends only on `velocity-api` (compile scope) and `netty-all` (provided scope). All access to Velocity internals (`VelocityServer`, `ConnectionManager`, `ConnectedPlayer`, `MinecraftConnection`) is done through reflection at runtime.

The general pattern for locating objects:

```
VelocityServer
  └─ cm (ConnectionManager)
       ├─ serverChannelInitializer (ServerChannelInitializerHolder)
       │    └─ ChannelInitializer<Channel>  ← wrapped by Field
       └─ endpoints (HashMultimap)
            └─ Endpoint objects
                 └─ ChannelFuture → ServerChannel  ← boss pipeline injected
```

For force-disconnecting players:

```
Player (ConnectedPlayer)
  └─ getConnection() → MinecraftConnection
       └─ getChannel() → Netty Channel → close()
```

Each step has fallback strategies (field name lookup, type name matching, brute-force scan) to handle potential obfuscation or version differences.

### Connection Lifecycle Under Field

```
TCP SYN →
  ┌─ Boss pipeline: field-acceptor.channelRead()
  │   Check vanish (whitelist-aware), IP ban, rate limit
  │   If blocked: channel.unsafe().closeForcibly() → TCP RST, done.
  │
  └─ Accepted: pass to ServerBootstrapAcceptor
      │
      ChannelInitializer wrapper (Field):
      │   Check vanish, IP ban, rate limit again
      │   If blocked: channel.close() → done.
      │   Else: invoke original Velocity ChannelInitializer
      │         Add field-connection-filter to head of child pipeline
      │
      ├─ Velocity handshake/ping handling
      │   Event layer: ConnectionHandshakeEvent, ProxyPingEvent
      │   ConnectionListener checks vanish/ban, closes channel if needed
      │
      ├─ PreLoginEvent
      │   LoginListener checks:
      │     - IP ban
      │     - Name ban → auto-ban IP, close
      │     - Vanish (whitelist-aware)
      │
      ├─ GameProfileRequestEvent (has UUID now)
      │   LoginListener checks:
      │     - Name ban (recheck with authenticated name)
      │     - UUID evasion: if UUID has banned IPs, auto-ban current IP
      │
      └─ LoginEvent
          Final checks:
            - force disconnect if banned
            - deny join when per-IP online limit is exceeded (UUID-aware)
```

## Rate Limiting

Per-IP rate limiting uses a simple sliding-window counter:

```java
private static class RateBucket {
    AtomicInteger count;   // connections in current window
    AtomicLong windowStart; // timestamp of window start
}
```

Each IP gets a `RateBucket` in a `ConcurrentHashMap`. On each new connection, if more than 1 second has elapsed since `windowStart`, the counter resets. If the count exceeds `max-connections-per-second`, the connection is rejected.

Two configurable actions on exceed:
- `close` — silently close the connection
- `ban` — close and auto-ban the IP for a configurable duration

Stale buckets (no activity for 10+ seconds) are cleaned up by a periodic task.

## Per-IP Online Limit

Field can limit how many players from the same IP may be online at the same time (`general.max-online-per-ip`).

- `0` = disabled
- `n > 0` = at most `n` online players for one IP
- Enforced at `LoginEvent` (UUID-aware) to avoid false rejects during duplicate-session replacement/reconnects
- If a new player exceeds this limit, login is denied (not force-closed) with the Velocity localization key:
  `velocity.error.already-connected-proxy`

## Ban Storage

Bans are stored in a SQLite database (`bans.db`) with WAL journal mode for concurrent read performance. The schema:

```sql
CREATE TABLE bans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,        -- 'IP' or 'NAME'
    value TEXT NOT NULL,       -- IP address or lowercase player name
    player_name TEXT,          -- associated player (for IP bans via player)
    player_uuid TEXT,          -- associated UUID
    ban_timestamp INTEGER,
    expiry_timestamp INTEGER,  -- -1 for permanent
    banned_by TEXT,
    UNIQUE(type, value)
);
```

On startup, all active (non-expired) bans are loaded into two `ConcurrentHashMap` caches:
- `ipBans`: IP string → BanEntry
- `nameBans`: lowercase name → BanEntry

A third map `uuidToIps` (UUID → Set\<IP\>) enables ban evasion detection. All runtime lookups are O(1) against the cache; the database is only hit on writes.

Expired bans are cleaned from the database on startup and on cache refresh (`/field reload`).

## Ban Evasion Detection

When a player authenticates (at the `GameProfileRequestEvent` stage, where the UUID becomes available), Field checks if that UUID has any existing IP bans via the `uuidToIps` map. If the player is connecting from a **new** IP that isn't banned, that IP is automatically added to the ban list with `bannedBy = "Field-Evasion"`.

The same logic applies to name bans: if a name-banned player connects, their IP is automatically banned with `bannedBy = "Field-NameBan"` at the `PreLoginEvent` stage (before authentication completes).

## Vanish Mode

When vanish is enabled, **all** new TCP connections are rejected at the Netty level. Existing connections from non-whitelisted IPs are force-closed. The server appears completely offline — no ping response, no connection timeout message, just a refused connection.

Whitelisted IPs (stored in `plugins/field/whitelist.txt`, one per line) bypass vanish checks at every layer. This allows administrators to connect while the server is invisible to everyone else.

## Force Disconnect

`/field kick` and internal force-disconnect operations don't send a Minecraft disconnect packet. Instead, Field locates the player's Netty `Channel` through reflection:

```
ConnectedPlayer → MinecraftConnection → Channel.close()
```

This immediately terminates the TCP connection. The client sees "Connection Lost" with no kick message, similar to a network failure.

For IP-based kicks, Field iterates its tracked channel map and closes all channels associated with the target IP.

## Channel Tracking

Every connection that passes through the ChannelInitializer wrapper is tracked in a `ConcurrentHashMap<String, Set<Channel>>` keyed by IP. A `closeFuture` listener on each channel automatically removes it from the map when the connection ends. This provides:

- IP-based force disconnect (`forceCloseByIp`)
- Total connection count (`AtomicInteger`, incremented on init, decremented on close)
- Active IP count for the status command

## Commands

```
/field kick <player|IP>           Force TCP disconnect
/field ban <player|IP> [duration] Ban (auto-detects IP vs name)
/field unban <player|IP>          Remove ban
/field banlist [page|clear]       View or clear all bans
/field vanish [on|off]            Toggle server visibility
/field whitelist <add|remove|list|reload>  Manage vanish whitelist
/field reload                     Reload config and whitelist
/field status                     Runtime statistics
```

`/field ban` auto-detects the target type:
- Input matching `^\d{1,3}(\.\d{1,3}){3}$` or containing `:` with hex chars → IP ban
- Online player name → resolve IP, ban the IP
- Offline player name → name ban (IP will be auto-banned on connect attempt)

Duration format: `30s`, `5m`, `2h`, `7d`, `4w`, or `permanent` (default).

## Permissions

| Node | Scope |
|------|-------|
| `field.admin` | All commands |
| `field.kick` | `/field kick` |
| `field.ban` | `/field ban`, `/field unban`, `/field banlist clear` |
| `field.banlist` | `/field banlist` |
| `field.vanish` | `/field vanish`, `/field whitelist` |
| `field.reload` | `/field reload` |

Console always has full access.

## Configuration

`plugins/field/config.toml`:

```toml
[general]
prefix = "<gradient:#ff6b6b:#ee5a24>Field</gradient> <dark_gray>» <reset>"
vanish-on-startup = false
log-blocked-connections = true
max-online-per-ip = 0

[rate-limit]
max-connections-per-second = 4
exceed-action = "close"        # "close" or "ban"
auto-ban-duration = "5m"       # used when exceed-action = "ban"

[messages]
# All messages support MiniMessage formatting and {placeholder} substitution

[database]
file = "bans.db"
```

`plugins/field/whitelist.txt`:

```
# One IP per line, lines starting with # are comments
127.0.0.1
```

All configuration files support hot-reload via `/field reload`.

## Build

Requires JDK 17.

```sh
mvn clean package
```

Output: `target/field-1.1.0.jar`

The SQLite JDBC driver is shaded into the jar. Netty and the Velocity API are provided by the runtime environment.

## Runtime Requirements

- Velocity 3.4.0-SNAPSHOT (build 558 or compatible)
- Java 17+

The plugin uses `--add-opens`-style reflective access. If Velocity's internal class structure changes significantly, the reflection strategies may need updating — the plugin logs warnings when it can't locate expected fields and falls back to event-based filtering.
