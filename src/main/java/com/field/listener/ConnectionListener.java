package com.field.listener;

import com.field.FieldPlugin;
import com.velocitypowered.api.event.PostOrder;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.connection.ConnectionHandshakeEvent;
import com.velocitypowered.api.event.proxy.ProxyPingEvent;
import com.velocitypowered.api.proxy.InboundConnection;
import com.velocitypowered.api.proxy.ProxyServer;
import io.netty.channel.Channel;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Secondary defense layer at the Velocity event level.
 * Now whitelist-aware for vanish mode.
 */
public class ConnectionListener {

    private final FieldPlugin plugin;
    private final ProxyServer server;
    private final Logger logger;

    public ConnectionListener(FieldPlugin plugin, ProxyServer server, Logger logger) {
        this.plugin = plugin;
        this.server = server;
        this.logger = logger;
    }

    @Subscribe(order = PostOrder.FIRST)
    public void onHandshake(ConnectionHandshakeEvent event) {
        String ip = extractIp(event.getConnection());
        if (ip == null) return;

        if (shouldBlock(ip)) {
            forceClose(event.getConnection());
            if (plugin.getConfig().isLogBlockedConnections()) {
                logger.info("[Field] Event REJECT handshake from {} ({})", ip, blockReason(ip));
            }
        }
    }

    @Subscribe(order = PostOrder.FIRST)
    public void onPing(ProxyPingEvent event) {
        String ip = extractIp(event.getConnection());
        if (ip == null) return;

        if (shouldBlock(ip)) {
            forceClose(event.getConnection());
            if (plugin.getConfig().isLogBlockedConnections()) {
                logger.info("[Field] Event REJECT ping from {} ({})", ip, blockReason(ip));
            }
        }
    }

    private boolean shouldBlock(String ip) {
        if (plugin.getVanishManager().isVanished()) {
            // Allow whitelisted IPs
            if (plugin.getWhitelistManager().isWhitelisted(ip)) {
                return false;
            }
            return true;
        }
        return plugin.getBanManager().isIpBanned(ip);
    }

    private String blockReason(String ip) {
        if (plugin.getVanishManager().isVanished()) return "vanish";
        return "banned";
    }

    private void forceClose(InboundConnection connection) {
        Channel channel = findChannelDeep(connection, 5);
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