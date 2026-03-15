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
    private Field initializerField;

    private final ConcurrentHashMap<String, RateBucket> rateLimitMap = new ConcurrentHashMap<>();
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

        int incrementAndGet() {
            long now = System.currentTimeMillis();
            if (now - windowStart.get() >= 1000) {
                count.set(1);
                windowStart.set(now);
                return 1;
            }
            return count.incrementAndGet();
        }
    }

    private boolean checkRateLimit(String ip) {
        int maxPerSec = plugin.getConfig().getMaxConnectionsPerSecond();
        if (maxPerSec <= 0 || ip == null) return false;

        RateBucket bucket = rateLimitMap.computeIfAbsent(ip, k -> new RateBucket());
        int count = bucket.incrementAndGet();

        if (count > maxPerSec) {
            if (plugin.getConfig().isLogBlockedConnections()) {
                logger.info("[Field] RATE LIMITED {} ({}/s > {})", ip, count, maxPerSec);
            }
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

    private void startRateLimitCleanup() {
        plugin.getServer().getScheduler()
                .buildTask(plugin, () -> {
                    long now = System.currentTimeMillis();
                    rateLimitMap.entrySet().removeIf(e -> now - e.getValue().windowStart.get() > 10_000);
                })
                .repeat(10, TimeUnit.SECONDS)
                .schedule();
    }

    // =====================================================================
    //  Injection entry point
    // =====================================================================

    public void inject() {
        try {
            Object connectionManager = findConnectionManager(server);
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
                logger.info("[Field] Netty interceptor installed via ChannelInitializer wrapper.");
            }

        } catch (Exception e) {
            logger.error("[Field] Netty injection failed", e);
        }
    }

    // =====================================================================
    //  Wrap ServerChannelInitializerHolder
    //
    //  Key design for ViaVersion compatibility:
    //
    //  ViaVersion also wraps the ServerChannelInitializerHolder AFTER us.
    //  The chain becomes:
    //    holder.get() -> ViaVersion wrapper -> Field wrapper -> Original
    //
    //  When we BLOCK a connection, we must NOT invoke the chain at all,
    //  because ViaVersion's wrapper will try to find "minecraft-encoder"
    //  in the pipeline (which doesn't exist since we never called the
    //  original Velocity initializer).
    //
    //  When we ALLOW a connection, we must delegate to whatever is
    //  CURRENTLY in the holder (which includes ViaVersion's wrapper).
    //  But since we ARE the wrapper in the holder (or ViaVersion wraps us),
    //  we need to carefully avoid infinite recursion.
    //
    //  Solution: We store a reference to what was in the holder BEFORE us
    //  (the "fallback original"). When ViaVersion wraps the holder after us,
    //  the holder now contains ViaVersion's wrapper. On each connection:
    //    - Read current holder value
    //    - If it's different from ourselves -> call it (this is ViaVersion's wrapper)
    //    - If it IS ourselves -> call the fallback original
    //
    //  But there's a subtlety: ViaVersion's wrapper internally calls
    //  the "original" it captured, which is OUR wrapper. So the call chain
    //  actually becomes:
    //    holder.get() = ViaVersion wrapper
    //    ViaVersion wrapper calls its captured "original" = Field wrapper
    //    Field wrapper.initChannel() runs our checks
    //    If allowed: Field wrapper calls fallbackOriginal (Velocity's real init)
    //    ViaVersion's code then adds its handlers to the now-initialized pipeline
    //
    //  This works perfectly because:
    //  1. Field checks run FIRST (inside our initChannel)
    //  2. If blocked: we clear pipeline + closeForcibly, throw exception to abort
    //  3. If allowed: we call Velocity's original, then ViaVersion adds its stuff
    // =====================================================================

    private boolean wrapServerChannelInitializerHolder(Object connectionManager) {
        try {
            // Find the holder field
            Field holderField = null;
            for (Field f : getAllFields(connectionManager.getClass())) {
                f.setAccessible(true);
                Object val = f.get(connectionManager);
                if (val != null && val.getClass().getSimpleName().contains("ServerChannelInitializerHolder")) {
                    holderField = f;
                    break;
                }
            }

            if (holderField == null) {
                logger.error("[Field] Cannot find ServerChannelInitializerHolder.");
                return false;
            }

            holderField.setAccessible(true);
            this.initializerHolder = holderField.get(connectionManager);

            // Find the field inside the holder that stores the ChannelInitializer
            for (Field f : getAllFields(initializerHolder.getClass())) {
                f.setAccessible(true);
                Object val = f.get(initializerHolder);
                if (val instanceof ChannelInitializer<?>) {
                    this.initializerField = f;
                    break;
                }
            }

            // Read current initializer
            Object currentInit = readCurrentInitializer();
            if (!(currentInit instanceof ChannelInitializer<?>)) {
                logger.error("[Field] Cannot read ChannelInitializer from holder.");
                return false;
            }

            this.originalInitializer = currentInit;
            @SuppressWarnings("unchecked")
            ChannelInitializer<Channel> original = (ChannelInitializer<Channel>) currentInit;

            // Create wrapper
            ChannelInitializer<Channel> wrapper = createInitializerWrapper(original);

            // Write wrapper into the holder
            return writeInitializer(wrapper);

        } catch (Exception e) {
            logger.error("[Field] Failed to wrap initializer holder", e);
            return false;
        }
    }

    /**
     * Read the current ChannelInitializer from the holder.
     */
    private Object readCurrentInitializer() {
        // Try get() method
        try {
            Method getMethod = findGetMethod();
            if (getMethod != null) {
                getMethod.setAccessible(true);
                return getMethod.invoke(initializerHolder);
            }
        } catch (Exception ignored) {}

        // Try direct field
        if (initializerField != null) {
            try {
                return initializerField.get(initializerHolder);
            } catch (Exception ignored) {}
        }

        return null;
    }

    /**
     * Find the get() method on the holder.
     */
    private Method findGetMethod() {
        for (Method m : initializerHolder.getClass().getMethods()) {
            if (m.getParameterCount() == 0
                    && ChannelInitializer.class.isAssignableFrom(m.getReturnType())) {
                return m;
            }
        }
        try {
            return initializerHolder.getClass().getMethod("get");
        } catch (NoSuchMethodException ignored) {}
        return null;
    }

    /**
     * Write a new ChannelInitializer into the holder.
     */
    private boolean writeInitializer(ChannelInitializer<Channel> newInit) {
        // Try set() method
        for (Method m : initializerHolder.getClass().getMethods()) {
            if (m.getParameterCount() == 1 && m.getName().equals("set")) {
                try {
                    m.setAccessible(true);
                    m.invoke(initializerHolder, newInit);
                    logger.info("[Field] Set wrapper via holder.set().");
                    return true;
                } catch (Exception ignored) {}
            }
        }

        // Try direct field
        if (initializerField != null) {
            try {
                initializerField.set(initializerHolder, newInit);
                logger.info("[Field] Set wrapper via direct field.");
                return true;
            } catch (Exception ignored) {}
        }

        logger.error("[Field] Cannot write wrapper to holder.");
        return false;
    }

    /**
     * Check if this IP should be blocked.
     */
    private boolean shouldBlockConnection(String ip) {
        if (plugin.getVanishManager().isVanished()) {
            return ip == null || !plugin.getWhitelistManager().isWhitelisted(ip);
        }
        return ip != null && plugin.getBanManager().isIpBanned(ip);
    }

    /**
     * Creates the wrapper ChannelInitializer.
     *
     * @param fallbackOriginal The initializer that was in the holder when we wrapped it.
     *                         Used as fallback when no other plugin has re-wrapped after us.
     */
    private ChannelInitializer<Channel> createInitializerWrapper(ChannelInitializer<Channel> fallbackOriginal) {
        final ChannelInitializer<Channel> self;
        self = new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                String ip = extractIp(ch.remoteAddress());

                // === Check: rate limit ===
                if (checkRateLimit(ip)) {
                    rejectChannel(ch);
                    return;
                }

                // === Check: vanish + ban ===
                if (shouldBlockConnection(ip)) {
                    if (plugin.getConfig().isLogBlockedConnections()) {
                        String reason = plugin.getVanishManager().isVanished() ? "vanish" : "banned";
                        logger.info("[Field] INIT REJECT {} ({})", ip != null ? ip : "unknown", reason);
                    }
                    rejectChannel(ch);
                    return;
                }

                // === Connection ALLOWED ===
                // Call the original Velocity initializer (NOT the current holder value,
                // because ViaVersion calls us as part of its chain — if we read the holder
                // we'd get ViaVersion's wrapper and cause infinite recursion).
                invokeInitializer(fallbackOriginal, ch);

                // Add our ongoing filter if channel survived initialization
                if (ch.isActive()) {
                    totalConnectionCount.incrementAndGet();
                    ch.closeFuture().addListener(f -> totalConnectionCount.decrementAndGet());
                    if (ip != null) trackChannel(ip, ch);
                    try {
                        ch.pipeline().addFirst("field-connection-filter", new ChildChannelHandler(ip));
                    } catch (Exception ignored) {}
                }
            }
        };
        return self;
    }

    /**
     * Reject a channel completely.
     * 1. Clear all handlers from pipeline — prevents ViaVersion's handlerAdded()
     *    callback from finding "minecraft-encoder" and throwing NoSuchElementException.
     * 2. Close the underlying socket forcibly — no async close, no further events.
     */
    private static void rejectChannel(Channel ch) {
        // Step 1: Strip the pipeline clean
        try {
            ChannelPipeline pipeline = ch.pipeline();
            List<String> names = new ArrayList<>(pipeline.names());
            for (String name : names) {
                try {
                    pipeline.remove(name);
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}

        // Step 2: Forcibly close the socket
        try {
            ch.unsafe().closeForcibly();
        } catch (Exception e) {
            try { ch.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Invoke a ChannelInitializer's protected initChannel method via reflection.
     */
    private static void invokeInitializer(ChannelInitializer<Channel> initializer, Channel ch) throws Exception {
        Method initMethod = ChannelInitializer.class.getDeclaredMethod("initChannel", Channel.class);
        initMethod.setAccessible(true);
        initMethod.invoke(initializer, ch);
    }

    // =====================================================================
    //  Boss pipeline injection (TCP accept level)
    // =====================================================================

    private void scheduleServerChannelScan(Object connectionManager) {
        for (int delay : new int[]{2, 5, 10, 20}) {
            plugin.getServer().getScheduler()
                    .buildTask(plugin, () -> scanForServerChannels(connectionManager))
                    .delay(delay, TimeUnit.SECONDS)
                    .schedule();
        }
    }

    private void scanForServerChannels(Object connectionManager) {
        try {
            List<ServerChannel> found = new ArrayList<>();
            deepScanForServerChannels(connectionManager, found, 5, new IdentityHashMap<>());

            for (ServerChannel sc : found) {
                if (!injectedServerChannels.contains(sc)) {
                    injectServerChannel(sc);
                    injectedServerChannels.add(sc);
                    injected = true;
                    logger.info("[Field] Boss-pipeline injected into ServerChannel: {} (type: {})",
                            sc.localAddress(), sc.getClass().getSimpleName());
                }
            }
        } catch (Exception e) {
            logger.error("[Field] ServerChannel scan failed", e);
        }
    }

    /**
     * Recursively scan an object graph for ServerChannel instances.
     * Handles Guava Multimap, Map, Collection, ChannelFuture, and plain fields.
     */
    private void deepScanForServerChannels(Object obj, List<ServerChannel> result,
                                           int depth, IdentityHashMap<Object, Boolean> visited) {
        if (depth <= 0 || obj == null || visited.containsKey(obj)) return;
        visited.put(obj, Boolean.TRUE);

        // Direct matches
        if (obj instanceof ServerChannel sc) {
            if (!result.contains(sc)) result.add(sc);
            return;
        }
        if (obj instanceof ChannelFuture cf) {
            Channel ch = cf.channel();
            if (ch instanceof ServerChannel sc && !result.contains(sc)) result.add(sc);
            return;
        }
        if (obj instanceof Channel) return; // Non-server channel, skip

        Class<?> clazz = obj.getClass();
        String className = clazz.getName();

        // Skip JDK internals
        if (className.startsWith("java.") || className.startsWith("javax.")
                || className.startsWith("sun.") || className.startsWith("jdk.")
                || clazz.isPrimitive() || clazz.isEnum() || clazz.isArray()) {
            return;
        }

        // Guava Multimap — use reflection to call values()
        if (className.contains("Multimap") || className.contains("AbstractMultimap")) {
            try {
                Method valuesMethod = null;
                for (Method m : clazz.getMethods()) {
                    if (m.getName().equals("values") && m.getParameterCount() == 0) {
                        valuesMethod = m;
                        break;
                    }
                }
                if (valuesMethod != null) {
                    valuesMethod.setAccessible(true);
                    Object values = valuesMethod.invoke(obj);
                    if (values instanceof Collection<?> col) {
                        for (Object item : col) {
                            deepScanForServerChannels(item, result, depth - 1, visited);
                        }
                    }
                }
            } catch (Exception ignored) {}
            return; // Don't scan Multimap's internal fields
        }

        // Standard Map
        if (obj instanceof Map<?, ?> map) {
            for (Object v : map.values()) {
                deepScanForServerChannels(v, result, depth - 1, visited);
            }
            return;
        }

        // Standard Collection / Iterable
        if (obj instanceof Iterable<?> iter) {
            for (Object item : iter) {
                deepScanForServerChannels(item, result, depth - 1, visited);
            }
            return;
        }

        // Scan declared fields
        for (Field f : getAllFields(clazz)) {
            try {
                f.setAccessible(true);
                Object val = f.get(obj);
                if (val == null || visited.containsKey(val)) continue;

                if (val instanceof ServerChannel sc) {
                    if (!result.contains(sc)) result.add(sc);
                } else if (val instanceof ChannelFuture cf) {
                    Channel ch = cf.channel();
                    if (ch instanceof ServerChannel sc && !result.contains(sc)) result.add(sc);
                } else {
                    deepScanForServerChannels(val, result, depth - 1, visited);
                }
            } catch (Exception ignored) {}
        }
    }

    // =====================================================================
    //  Boss pipeline handler
    // =====================================================================

    private void injectServerChannel(ServerChannel serverChannel) {
        ChannelPipeline pipeline = serverChannel.pipeline();
        try {
            if (pipeline.get("field-acceptor") != null) pipeline.remove("field-acceptor");
        } catch (Exception ignored) {}

        pipeline.addFirst("field-acceptor", new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                if (msg instanceof Channel childChannel) {
                    String ip = extractIp(childChannel.remoteAddress());

                    if (checkRateLimit(ip)) {
                        closeForcibly(childChannel);
                        return;
                    }

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

    /**
     * Close a channel that may not be registered to an EventLoop yet.
     */
    private static void closeForcibly(Channel channel) {
        try {
            channel.unsafe().closeForcibly();
        } catch (Exception e) {
            try { channel.close(); } catch (Exception ignored) {}
        }
    }

    // =====================================================================
    //  Child channel ongoing filter
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
            if (shouldBlockConnection(ip)) { ctx.close(); return; }
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
        for (Channel ch : set) {
            if (ch.isActive()) { ch.close(); count++; }
        }
        return count;
    }

    public int forceCloseAll() {
        int count = 0;
        for (Set<Channel> set : activeChannels.values()) {
            for (Channel ch : set) {
                if (ch.isActive()) { ch.close(); count++; }
            }
        }
        activeChannels.clear();
        return count;
    }

    public int getTotalConnectionCount() { return totalConnectionCount.get(); }

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
                        mcConn = f.get(player);
                        break;
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
            try {
                if (sc.pipeline().get("field-acceptor") != null) {
                    sc.pipeline().remove("field-acceptor");
                }
            } catch (Exception ignored) {}
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
    //  Reflection utilities
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
                if (val != null && val.getClass().getSimpleName().contains("ConnectionManager"))
                    return val;
            } catch (Exception ignored) {}
        }
        return null;
    }

    private static Object getFieldValue(Object obj, String name) {
        Class<?> clazz = obj.getClass();
        while (clazz != null && clazz != Object.class) {
            try {
                Field f = clazz.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(obj);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            } catch (Exception e) {
                return null;
            }
        }
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