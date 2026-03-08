package com.field.manager;

import com.field.FieldPlugin;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import io.netty.channel.*;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionInterceptor {

    private final FieldPlugin plugin;
    private final ProxyServer server;
    private final Logger logger;

    private final ConcurrentHashMap<String, Set<Channel>> activeChannels = new ConcurrentHashMap<>();
    private volatile boolean injected = false;
    private final List<Channel> injectedServerChannels = Collections.synchronizedList(new ArrayList<>());

    private Object originalInitializer;
    private Object initializerHolder;

    // Rate limiting: IP -> RateBucket
    private final ConcurrentHashMap<String, RateBucket> rateLimitMap = new ConcurrentHashMap<>();

    // Total active connection count
    private final AtomicInteger totalConnectionCount = new AtomicInteger(0);

    public ConnectionInterceptor(FieldPlugin plugin, ProxyServer server, Logger logger) {
        this.plugin = plugin;
        this.server = server;
        this.logger = logger;
    }

    // =====================================================================
    //  Rate limiting
    // =====================================================================

    private static class RateBucket {
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicLong windowStart = new AtomicLong(System.currentTimeMillis());

        /**
         * Increment and check if over limit. Returns current count in window.
         */
        int incrementAndGet() {
            long now = System.currentTimeMillis();
            long start = windowStart.get();
            if (now - start >= 1000) {
                // New window
                count.set(1);
                windowStart.set(now);
                return 1;
            }
            return count.incrementAndGet();
        }
    }

    /**
     * Check rate limit for an IP. Returns true if the connection should be BLOCKED.
     */
    private boolean checkRateLimit(String ip) {
        int maxPerSec = plugin.getConfig().getMaxConnectionsPerSecond();
        if (maxPerSec <= 0 || ip == null) return false; // disabled

        RateBucket bucket = rateLimitMap.computeIfAbsent(ip, k -> new RateBucket());
        int count = bucket.incrementAndGet();

        if (count > maxPerSec) {
            // Exceeded
            if (plugin.getConfig().isLogBlockedConnections()) {
                logger.info("[Field] RATE LIMITED {} ({}/s > {})", ip, count, maxPerSec);
            }

            // Auto-ban if configured
            if ("ban".equalsIgnoreCase(plugin.getConfig().getExceedAction())) {
                long duration = BanManager.parseDuration(plugin.getConfig().getAutoBanDuration());
                if (!plugin.getBanManager().isIpBanned(ip)) {
                    plugin.getBanManager().banIp(ip, null, null, duration, "Field-RateLimit");
                    logger.info("[Field] Auto-banned {} for rate limit violation", ip);
                }
            }

            return true;
        }
        return false;
    }

    /**
     * Periodically clean stale rate limit entries.
     */
    private void startRateLimitCleanup() {
        plugin.getServer().getScheduler()
                .buildTask(plugin, () -> {
                    long now = System.currentTimeMillis();
                    rateLimitMap.entrySet().removeIf(entry ->
                            now - entry.getValue().windowStart.get() > 10_000);
                })
                .repeat(10, TimeUnit.SECONDS)
                .schedule();
    }

    // =====================================================================
    //  Injection
    // =====================================================================

    public void inject() {
        try {
            Object velocityServer = server;

            Object connectionManager = findConnectionManager(velocityServer);
            if (connectionManager == null) {
                logger.error("[Field] Cannot locate ConnectionManager. Netty injection FAILED.");
                return;
            }
            logger.info("[Field] Found ConnectionManager: {}", connectionManager.getClass().getName());

            boolean wrapped = wrapServerChannelInitializerHolder(connectionManager);

            scheduleServerChannelScan(connectionManager);
            startRateLimitCleanup();

            if (wrapped) {
                injected = true;
                logger.info("[Field] Netty interceptor installed.");
            }

        } catch (Exception e) {
            logger.error("[Field] Netty injection failed", e);
        }
    }

    private boolean wrapServerChannelInitializerHolder(Object connectionManager) {
        try {
            Field holderField = findField(connectionManager.getClass(), "serverChannelInitializer");
            if (holderField == null) {
                for (Field f : getAllFields(connectionManager.getClass())) {
                    f.setAccessible(true);
                    Object val = f.get(connectionManager);
                    if (val != null && val.getClass().getSimpleName().contains("ServerChannelInitializerHolder")) {
                        holderField = f;
                        break;
                    }
                }
            }

            if (holderField == null) {
                logger.error("[Field] Cannot find ServerChannelInitializerHolder.");
                return false;
            }

            holderField.setAccessible(true);
            this.initializerHolder = holderField.get(connectionManager);

            // get()
            Method getMethod = null;
            for (Method m : initializerHolder.getClass().getMethods()) {
                if (m.getParameterCount() == 0
                        && ChannelInitializer.class.isAssignableFrom(m.getReturnType())) {
                    getMethod = m;
                    break;
                }
            }
            if (getMethod == null) {
                try { getMethod = initializerHolder.getClass().getMethod("get"); }
                catch (NoSuchMethodException ignored) {}
            }

            if (getMethod == null) {
                for (Field f : getAllFields(initializerHolder.getClass())) {
                    f.setAccessible(true);
                    Object val = f.get(initializerHolder);
                    if (val instanceof ChannelInitializer<?>) {
                        this.originalInitializer = val;
                        @SuppressWarnings("unchecked")
                        ChannelInitializer<Channel> orig = (ChannelInitializer<Channel>) val;
                        f.set(initializerHolder, createInitializerWrapper(orig));
                        return true;
                    }
                }
                return false;
            }

            getMethod.setAccessible(true);
            Object currentInit = getMethod.invoke(initializerHolder);
            if (!(currentInit instanceof ChannelInitializer<?>)) return false;

            this.originalInitializer = currentInit;
            @SuppressWarnings("unchecked")
            ChannelInitializer<Channel> original = (ChannelInitializer<Channel>) currentInit;
            ChannelInitializer<Channel> wrapper = createInitializerWrapper(original);

            // set()
            Method setMethod = null;
            for (Method m : initializerHolder.getClass().getMethods()) {
                if (m.getParameterCount() == 1 && m.getName().equals("set")) {
                    setMethod = m;
                    break;
                }
            }
            if (setMethod != null) {
                setMethod.setAccessible(true);
                setMethod.invoke(initializerHolder, wrapper);
                return true;
            }

            for (Field f : getAllFields(initializerHolder.getClass())) {
                f.setAccessible(true);
                if (f.get(initializerHolder) == currentInit) {
                    f.set(initializerHolder, wrapper);
                    return true;
                }
            }
            return false;

        } catch (Exception e) {
            logger.error("[Field] Failed to wrap initializer holder", e);
            return false;
        }
    }

    /**
     * Check if IP should be blocked considering vanish + whitelist.
     */
    private boolean shouldBlockConnection(String ip) {
        // Vanish mode check (whitelist aware)
        if (plugin.getVanishManager().isVanished()) {
            if (ip != null && plugin.getWhitelistManager().isWhitelisted(ip)) {
                return false; // whitelisted, allow through
            }
            return true; // vanish on, not whitelisted
        }

        // IP ban check
        if (ip != null && plugin.getBanManager().isIpBanned(ip)) {
            return true;
        }

        return false;
    }

    private ChannelInitializer<Channel> createInitializerWrapper(ChannelInitializer<Channel> original) {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                String ip = extractIp(ch.remoteAddress());

                // Rate limit check
                if (checkRateLimit(ip)) {
                    ch.close();
                    return;
                }

                // Vanish + Ban check
                if (shouldBlockConnection(ip)) {
                    if (plugin.getConfig().isLogBlockedConnections()) {
                        String reason = plugin.getVanishManager().isVanished() ? "vanish" : "banned";
                        logger.info("[Field] INIT REJECT {} ({})", ip != null ? ip : "unknown", reason);
                    }
                    ch.close();
                    return;
                }

                // Call original
                Method initMethod = ChannelInitializer.class.getDeclaredMethod("initChannel", Channel.class);
                initMethod.setAccessible(true);
                initMethod.invoke(original, ch);

                if (ch.isActive()) {
                    totalConnectionCount.incrementAndGet();
                    ch.closeFuture().addListener(f -> totalConnectionCount.decrementAndGet());

                    if (ip != null) trackChannel(ip, ch);
                    ch.pipeline().addFirst("field-connection-filter", new ChildChannelHandler(ip));
                }
            }
        };
    }

    // =====================================================================
    //  ServerChannel scan + boss pipeline
    // =====================================================================

    private void scheduleServerChannelScan(Object connectionManager) {
        plugin.getServer().getScheduler()
                .buildTask(plugin, () -> scanForServerChannels(connectionManager))
                .delay(2, TimeUnit.SECONDS).schedule();
        plugin.getServer().getScheduler()
                .buildTask(plugin, () -> scanForServerChannels(connectionManager))
                .delay(5, TimeUnit.SECONDS).schedule();
    }

    private void scanForServerChannels(Object connectionManager) {
        try {
            List<ServerChannel> found = new ArrayList<>();
            for (Field f : getAllFields(connectionManager.getClass())) {
                try {
                    f.setAccessible(true);
                    Object val = f.get(connectionManager);
                    if (val == null) continue;
                    if (val.getClass().getSimpleName().contains("Multimap")) {
                        scanMultimap(val, found);
                    } else if (val instanceof Map<?, ?> map) {
                        for (Object v : map.values())
                            findServerChannelsInObject(v, found, 3, new HashSet<>());
                    } else if (val instanceof Collection<?> col) {
                        for (Object v : col)
                            findServerChannelsInObject(v, found, 3, new HashSet<>());
                    }
                } catch (Exception ignored) {}
            }

            for (ServerChannel sc : found) {
                if (!injectedServerChannels.contains(sc)) {
                    injectServerChannel(sc);
                    injectedServerChannels.add(sc);
                    injected = true;
                    logger.info("[Field] Boss-pipeline injected: {}", sc.localAddress());
                }
            }
        } catch (Exception e) {
            logger.error("[Field] ServerChannel scan failed", e);
        }
    }

    private void scanMultimap(Object multimap, List<ServerChannel> result) {
        try {
            Method valuesMethod = findMethod(multimap.getClass(), "values", 0);
            if (valuesMethod != null) {
                valuesMethod.setAccessible(true);
                Object values = valuesMethod.invoke(multimap);
                if (values instanceof Collection<?> col) {
                    for (Object endpoint : col)
                        findServerChannelsInObject(endpoint, result, 3, new HashSet<>());
                }
            }
        } catch (Exception ignored) {}
    }

    private void findServerChannelsInObject(Object obj, List<ServerChannel> result,
                                            int depth, Set<Object> visited) {
        if (depth <= 0 || obj == null || visited.contains(obj)) return;
        visited.add(obj);
        if (obj instanceof ServerChannel sc) { if (!result.contains(sc)) result.add(sc); return; }
        if (obj instanceof ChannelFuture cf) {
            Channel ch = cf.channel();
            if (ch instanceof ServerChannel sc && !result.contains(sc)) result.add(sc);
            return;
        }
        if (obj instanceof Channel) return;
        if (isJdkType(obj.getClass())) return;

        for (Field f : getAllFields(obj.getClass())) {
            try {
                f.setAccessible(true);
                Object val = f.get(obj);
                if (val == null || visited.contains(val)) continue;
                if (val instanceof ServerChannel sc) { if (!result.contains(sc)) result.add(sc); }
                else if (val instanceof ChannelFuture cf) {
                    Channel ch = cf.channel();
                    if (ch instanceof ServerChannel sc && !result.contains(sc)) result.add(sc);
                } else if (val instanceof Channel ch) {
                    if (ch instanceof ServerChannel sc && !result.contains(sc)) result.add(sc);
                } else if (!isJdkType(val.getClass())) {
                    findServerChannelsInObject(val, result, depth - 1, visited);
                }
            } catch (Exception ignored) {}
        }
    }

    private void injectServerChannel(ServerChannel serverChannel) {
        ChannelPipeline pipeline = serverChannel.pipeline();
        if (pipeline.get("field-acceptor") != null) pipeline.remove("field-acceptor");

        pipeline.addFirst("field-acceptor", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof Channel childChannel) {
                    String ip = extractIp(childChannel.remoteAddress());

                    // Rate limit
                    if (checkRateLimit(ip)) {
                        closeForcibly(childChannel);
                        return;
                    }

                    // Vanish + ban
                    if (shouldBlockConnection(ip)) {
                        if (plugin.getConfig().isLogBlockedConnections()) {
                            String reason = plugin.getVanishManager().isVanished() ? "vanish" : "banned";
                            logger.info("[Field] TCP REJECT {} ({})", ip != null ? ip : "?", reason);
                        }
                        closeForcibly(childChannel);
                        return;
                    }
                }
                super.channelRead(ctx, msg);
            }
        });
    }

    private static void closeForcibly(Channel channel) {
        try {
            channel.unsafe().closeForcibly();
        } catch (Exception e) {
            try { if (channel instanceof java.io.Closeable c) c.close(); }
            catch (Exception ignored) {}
        }
    }

    // =====================================================================
    //  Child channel handler
    // =====================================================================

    private class ChildChannelHandler extends ChannelInboundHandlerAdapter {
        private volatile String ip;

        ChildChannelHandler(String ip) { this.ip = ip; }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            if (this.ip == null) this.ip = extractIp(ctx.channel().remoteAddress());
            if (shouldBlockConnection(ip)) { ctx.close(); return; }
            if (ip != null) trackChannel(ip, ctx.channel());
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            if (ip != null) untrackChannel(ip, ctx.channel());
            super.channelInactive(ctx);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (shouldBlockConnection(ip)) {
                ctx.close();
                return;
            }
            super.channelRead(ctx, msg);
        }
    }

    // =====================================================================
    //  Channel tracking
    // =====================================================================

    private void trackChannel(String ip, Channel channel) {
        activeChannels.computeIfAbsent(ip, k -> ConcurrentHashMap.newKeySet()).add(channel);
        channel.closeFuture().addListener(f -> untrackChannel(ip, channel));
    }

    private void untrackChannel(String ip, Channel channel) {
        Set<Channel> set = activeChannels.get(ip);
        if (set != null) {
            set.remove(channel);
            if (set.isEmpty()) activeChannels.remove(ip, set);
        }
    }

    public int forceCloseByIp(String ip) {
        Set<Channel> set = activeChannels.remove(ip);
        if (set == null) return 0;
        int count = 0;
        for (Channel ch : set) { if (ch.isActive()) { ch.close(); count++; } }
        return count;
    }

    public int forceCloseAll() {
        int count = 0;
        for (Set<Channel> set : activeChannels.values())
            for (Channel ch : set)
                if (ch.isActive()) { ch.close(); count++; }
        activeChannels.clear();
        return count;
    }

    public int getTotalConnectionCount() {
        return totalConnectionCount.get();
    }

    // =====================================================================
    //  Force disconnect player
    // =====================================================================

    public static void forceDisconnectPlayer(Player player) {
        try {
            Channel channel = findPlayerChannel(player);
            if (channel != null) { channel.close(); return; }
        } catch (Exception ignored) {}
        try { player.disconnect(net.kyori.adventure.text.Component.empty()); }
        catch (Exception ignored) {}
    }

    private static Channel findPlayerChannel(Player player) {
        try {
            Object mcConn = null;
            try {
                Method m = player.getClass().getMethod("getConnection");
                m.setAccessible(true);
                mcConn = m.invoke(player);
            } catch (NoSuchMethodException ignored) {}
            if (mcConn == null) mcConn = getFieldValue(player, "connection");
            if (mcConn == null) {
                for (Field f : getAllFields(player.getClass())) {
                    f.setAccessible(true);
                    if (f.getType().getSimpleName().contains("MinecraftConnection")) {
                        mcConn = f.get(player); break;
                    }
                }
            }
            if (mcConn == null) return null;

            try {
                Method m = mcConn.getClass().getMethod("getChannel");
                m.setAccessible(true);
                Object ch = m.invoke(mcConn);
                if (ch instanceof Channel c) return c;
            } catch (NoSuchMethodException ignored) {}

            Object ch = getFieldValue(mcConn, "channel");
            if (ch instanceof Channel c) return c;

            for (Field f : getAllFields(mcConn.getClass())) {
                f.setAccessible(true);
                Object val = f.get(mcConn);
                if (val instanceof Channel c) return c;
            }
        } catch (Exception ignored) {}
        return null;
    }

    // =====================================================================
    //  Cleanup
    // =====================================================================

    public void uninject() {
        for (Channel sc : injectedServerChannels) {
            try { if (sc.pipeline().get("field-acceptor") != null) sc.pipeline().remove("field-acceptor"); }
            catch (Exception ignored) {}
        }
        injectedServerChannels.clear();
        if (initializerHolder != null && originalInitializer != null) {
            try {
                for (Method m : initializerHolder.getClass().getMethods()) {
                    if (m.getParameterCount() == 1 && m.getName().equals("set")) {
                        m.setAccessible(true);
                        m.invoke(initializerHolder, originalInitializer);
                        break;
                    }
                }
            } catch (Exception ignored) {}
        }
        activeChannels.clear();
        injected = false;
    }

    public boolean isInjected() { return injected; }
    public Map<String, Set<Channel>> getActiveChannels() { return activeChannels; }

    // =====================================================================
    //  Reflection
    // =====================================================================

    static String extractIp(SocketAddress address) {
        if (address instanceof InetSocketAddress inet) {
            InetAddress a = inet.getAddress();
            return a != null ? a.getHostAddress() : null;
        }
        return null;
    }

    private Object findConnectionManager(Object velocityServer) {
        Object cm = getFieldValue(velocityServer, "cm");
        if (cm != null && cm.getClass().getSimpleName().contains("ConnectionManager")) return cm;
        for (Field f : getAllFields(velocityServer.getClass())) {
            try {
                f.setAccessible(true);
                Object val = f.get(velocityServer);
                if (val != null && val.getClass().getSimpleName().contains("ConnectionManager")) return val;
            } catch (Exception ignored) {}
        }
        return null;
    }

    private static Field findField(Class<?> clazz, String name) {
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            try { Field f = current.getDeclaredField(name); f.setAccessible(true); return f; }
            catch (NoSuchFieldException ignored) { current = current.getSuperclass(); }
        }
        return null;
    }

    private static Object getFieldValue(Object obj, String name) {
        Class<?> clazz = obj.getClass();
        while (clazz != null && clazz != Object.class) {
            try { Field f = clazz.getDeclaredField(name); f.setAccessible(true); return f.get(obj); }
            catch (NoSuchFieldException e) { clazz = clazz.getSuperclass(); }
            catch (Exception e) { return null; }
        }
        return null;
    }

    private static Method findMethod(Class<?> clazz, String name, int paramCount) {
        for (Method m : clazz.getMethods())
            if (m.getName().equals(name) && m.getParameterCount() == paramCount) return m;
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

    private static boolean isJdkType(Class<?> clazz) {
        String name = clazz.getName();
        return name.startsWith("java.") || name.startsWith("javax.")
                || name.startsWith("sun.") || name.startsWith("jdk.")
                || clazz.isPrimitive() || clazz.isEnum() || clazz.isArray()
                || name.startsWith("com.google.");
    }
}