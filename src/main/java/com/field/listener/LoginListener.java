package com.field.listener;

import com.field.FieldPlugin;
import com.field.manager.ConnectionInterceptor;
import com.velocitypowered.api.event.PostOrder;
import com.velocitypowered.api.event.ResultedEvent;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.LoginEvent;
import com.velocitypowered.api.event.connection.PreLoginEvent;
import com.velocitypowered.api.event.player.GameProfileRequestEvent;
import com.velocitypowered.api.proxy.InboundConnection;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import io.netty.channel.Channel;
import net.kyori.adventure.text.Component;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Handles login events:
 * - IP ban enforcement
 * - Name ban enforcement (offline player name ban)
 * - Ban evasion detection (UUID-based)
 * - Per-IP online player limit
 */
public class LoginListener {

    private static final String VELOCITY_LANG_BUNDLE = "com.velocitypowered.proxy.l10n.messages";
    private static final String ALREADY_CONNECTED_PROXY_KEY = "velocity.error.already-connected-proxy";

    private final FieldPlugin plugin;
    private final ProxyServer server;
    private final Logger logger;

    public LoginListener(FieldPlugin plugin, ProxyServer server, Logger logger) {
        this.plugin = plugin;
        this.server = server;
        this.logger = logger;
    }

    @Subscribe(order = PostOrder.FIRST)
    public void onPreLogin(PreLoginEvent event) {
        InboundConnection conn = event.getConnection();
        String ip = extractIp(conn);
        String username = event.getUsername();

        // Vanish check (whitelist-aware)
        if (plugin.getVanishManager().isVanished()) {
            if (ip == null || !plugin.getWhitelistManager().isWhitelisted(ip)) {
                event.setResult(PreLoginEvent.PreLoginComponentResult.denied(Component.empty()));
                forceClose(conn);
                return;
            }
        }

        // IP ban check
        if (ip != null && plugin.getBanManager().isIpBanned(ip)) {
            event.setResult(PreLoginEvent.PreLoginComponentResult.denied(Component.empty()));
            forceClose(conn);
            if (plugin.getConfig().isLogBlockedConnections()) {
                logger.info("[Field] PreLogin REJECT {} (IP banned)", ip);
            }
            return;
        }

        // Name ban check - ban IP and disconnect
        if (username != null && plugin.getBanManager().isNameBanned(username)) {
            logger.info("[Field] Name-banned player '{}' attempting login from IP {}", username, ip);

            // Ban their IP too
            if (ip != null && !plugin.getBanManager().isIpBanned(ip)) {
                plugin.getBanManager().banIp(ip, username, null, -1, "Field-NameBan");
                logger.info("[Field] Auto-banned IP {} (name-banned player '{}')", ip, username);
            }

            event.setResult(PreLoginEvent.PreLoginComponentResult.denied(Component.empty()));
            forceClose(conn);
            if (ip != null) {
                plugin.getConnectionInterceptor().forceCloseByIp(ip);
            }
            return;
        }

        // Per-IP online player limit (deny with Velocity language message, do not force-close)
        int maxOnlinePerIp = plugin.getConfig().getMaxOnlinePerIp();
        if (maxOnlinePerIp > 0 && ip != null) {
            int currentOnline = countOnlinePlayersByIp(ip, null);
            if (currentOnline >= maxOnlinePerIp) {
                event.setResult(PreLoginEvent.PreLoginComponentResult.denied(alreadyConnectedProxyMessage()));
                if (plugin.getConfig().isLogBlockedConnections()) {
                    logger.info("[Field] PreLogin REJECT {} (per-ip online limit: {}/{})",
                            ip, currentOnline, maxOnlinePerIp);
                }
            }
        }
    }

    @Subscribe(order = PostOrder.FIRST)
    public void onGameProfile(GameProfileRequestEvent event) {
        if (event.getGameProfile() == null) return;

        String uuid = event.getGameProfile().getId().toString();
        String playerName = event.getGameProfile().getName();
        InboundConnection conn = event.getConnection();
        String ip = extractIp(conn);

        if (ip == null || uuid == null) return;

        // Name ban check again (in case username changed during auth)
        if (playerName != null && plugin.getBanManager().isNameBanned(playerName)) {
            if (!plugin.getBanManager().isIpBanned(ip)) {
                plugin.getBanManager().banIp(ip, playerName, uuid, -1, "Field-NameBan");
                logger.info("[Field] Auto-banned IP {} for name-banned player '{}'", ip, playerName);
            }
            forceClose(conn);
            plugin.getConnectionInterceptor().forceCloseByIp(ip);
            return;
        }

        // UUID evasion check
        if (plugin.getBanManager().isPlayerUuidBanned(uuid) && !plugin.getBanManager().isIpBanned(ip)) {
            Set<String> bannedIps = plugin.getBanManager().getBannedIpsForUuid(uuid);
            plugin.getBanManager().banIp(ip, playerName, uuid, -1, "Field-Evasion");
            logger.info("[Field] Auto-banned IP {} for evading player {} (UUID: {}, banned IPs: {})",
                    ip, playerName, uuid, bannedIps);
            forceClose(conn);
            plugin.getConnectionInterceptor().forceCloseByIp(ip);
        }
    }

    @Subscribe(order = PostOrder.FIRST)
    public void onLogin(LoginEvent event) {
        Player player = event.getPlayer();
        String ip = extractPlayerIp(player);
        String uuid = player.getUniqueId().toString();
        String playerName = player.getUsername();

        if (ip == null) return;

        // Name ban final check
        if (plugin.getBanManager().isNameBanned(playerName)) {
            if (!plugin.getBanManager().isIpBanned(ip)) {
                plugin.getBanManager().banIp(ip, playerName, uuid, -1, "Field-NameBan");
                logger.info("[Field] Auto-banned IP {} at login for name-banned '{}'", ip, playerName);
            }
            event.setResult(ResultedEvent.ComponentResult.denied(Component.empty()));
            ConnectionInterceptor.forceDisconnectPlayer(player);
            plugin.getConnectionInterceptor().forceCloseByIp(ip);
            return;
        }

        // UUID evasion final check
        if (plugin.getBanManager().isPlayerUuidBanned(uuid)) {
            if (!plugin.getBanManager().isIpBanned(ip)) {
                plugin.getBanManager().banIp(ip, playerName, uuid, -1, "Field-Evasion");
                logger.info("[Field] Auto-banned IP {} at login for evading player '{}'", ip, playerName);
            }
            event.setResult(ResultedEvent.ComponentResult.denied(Component.empty()));
            ConnectionInterceptor.forceDisconnectPlayer(player);
            plugin.getConnectionInterceptor().forceCloseByIp(ip);
            return;
        }

        // IP ban final check
        if (plugin.getBanManager().isIpBanned(ip)) {
            event.setResult(ResultedEvent.ComponentResult.denied(Component.empty()));
            ConnectionInterceptor.forceDisconnectPlayer(player);
            return;
        }

        // Per-IP online player limit (deny with Velocity language message, do not force-close)
        int maxOnlinePerIp = plugin.getConfig().getMaxOnlinePerIp();
        if (maxOnlinePerIp > 0) {
            int currentOnline = countOnlinePlayersByIp(ip, player.getUniqueId());
            if (currentOnline >= maxOnlinePerIp) {
                event.setResult(ResultedEvent.ComponentResult.denied(alreadyConnectedProxyMessage()));
                if (plugin.getConfig().isLogBlockedConnections()) {
                    logger.info("[Field] Login REJECT {} ({}) (per-ip online limit: {}/{})",
                            playerName, ip, currentOnline, maxOnlinePerIp);
                }
            }
        }
    }

    private Component alreadyConnectedProxyMessage() {
        return Component.text(resolveVelocityMessage(ALREADY_CONNECTED_PROXY_KEY));
    }

    private String resolveVelocityMessage(String key) {
        ClassLoader velocityClassLoader = server.getClass().getClassLoader();
        try {
            ResourceBundle bundle = ResourceBundle.getBundle(
                    VELOCITY_LANG_BUNDLE, Locale.getDefault(), velocityClassLoader);
            if (bundle.containsKey(key)) {
                return bundle.getString(key);
            }
        } catch (Exception ignored) {}

        try {
            ResourceBundle fallback = ResourceBundle.getBundle(
                    VELOCITY_LANG_BUNDLE, Locale.ENGLISH, velocityClassLoader);
            if (fallback.containsKey(key)) {
                return fallback.getString(key);
            }
        } catch (Exception ignored) {}

        return "You are already connected to this proxy!";
    }

    private int countOnlinePlayersByIp(String ip, UUID excludeUuid) {
        if (ip == null) return 0;
        int count = 0;
        for (Player online : server.getAllPlayers()) {
            if (excludeUuid != null && excludeUuid.equals(online.getUniqueId())) {
                continue;
            }
            String onlineIp = extractPlayerIp(online);
            if (ip.equals(onlineIp)) {
                count++;
            }
        }
        return count;
    }

    private void forceClose(InboundConnection conn) {
        Channel channel = findChannelDeep(conn, 5);
        if (channel != null) channel.close();
    }

    private Channel findChannelDeep(Object obj, int depth) {
        if (depth <= 0 || obj == null) return null;
        if (obj instanceof Channel c) return c;
        try {
            for (Field f : getAllFields(obj.getClass())) {
                f.setAccessible(true);
                Object val;
                try { val = f.get(obj); } catch (Exception e) { continue; }
                if (val == null || val == obj) continue;
                if (val instanceof Channel c) return c;
                String cn = val.getClass().getName();
                if (!cn.startsWith("java.") && !cn.startsWith("javax.")
                        && !cn.startsWith("sun.") && !cn.startsWith("jdk.")
                        && !val.getClass().isPrimitive()) {
                    Channel found = findChannelDeep(val, depth - 1);
                    if (found != null) return found;
                }
            }
        } catch (Exception ignored) {}
        return null;
    }

    private String extractIp(InboundConnection conn) {
        try {
            InetSocketAddress addr = conn.getRemoteAddress();
            if (addr != null && addr.getAddress() != null) return addr.getAddress().getHostAddress();
        } catch (Exception ignored) {}
        return null;
    }

    private String extractPlayerIp(Player player) {
        try {
            InetSocketAddress addr = player.getRemoteAddress();
            if (addr != null && addr.getAddress() != null) return addr.getAddress().getHostAddress();
        } catch (Exception ignored) {}
        return null;
    }

    private static Field[] getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            try { Collections.addAll(fields, current.getDeclaredFields()); }
            catch (Exception ignored) {}
            current = current.getSuperclass();
        }
        return fields.toArray(new Field[0]);
    }
}
