package com.field.database;

import com.field.config.FieldConfig;
import com.field.model.BanEntry;
import com.field.model.BanEntry.BanType;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class BanDatabase {

    private final Path dataDirectory;
    private final FieldConfig config;
    private final Logger logger;
    private Connection connection;

    // IP bans: ip -> BanEntry
    private final ConcurrentHashMap<String, BanEntry> ipBans = new ConcurrentHashMap<>();
    // Name bans: lowercase name -> BanEntry
    private final ConcurrentHashMap<String, BanEntry> nameBans = new ConcurrentHashMap<>();
    // UUID -> set of banned IPs (for evasion detection)
    private final ConcurrentHashMap<String, Set<String>> uuidToIps = new ConcurrentHashMap<>();

    public BanDatabase(Path dataDirectory, FieldConfig config, Logger logger) {
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.logger = logger;
    }

    public synchronized void initialize() {
        try {
            try {
                Class.forName("org.sqlite.JDBC");
            } catch (ClassNotFoundException e) {
                logger.error("[Field] SQLite JDBC driver not found!", e);
                return;
            }

            String dbPath = dataDirectory.resolve(config.getDatabaseFile()).toAbsolutePath().toString();
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=NORMAL");
                stmt.execute("PRAGMA cache_size=10000");
            }

            createTables();
            cleanExpired();
            loadCache();

            logger.info("[Field] Database initialized. {} IP bans, {} name bans loaded.",
                    ipBans.size(), nameBans.size());
        } catch (SQLException e) {
            logger.error("[Field] Failed to initialize database", e);
        }
    }

    private void createTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Drop old table if schema changed
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS bans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL DEFAULT 'IP',
                    value TEXT NOT NULL,
                    player_name TEXT,
                    player_uuid TEXT,
                    ban_timestamp INTEGER NOT NULL,
                    expiry_timestamp INTEGER NOT NULL DEFAULT -1,
                    banned_by TEXT NOT NULL DEFAULT 'Console',
                    UNIQUE(type, value)
                )
            """);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_type_value ON bans(type, value)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_uuid ON bans(player_uuid)");
        }
    }

    private synchronized void loadCache() throws SQLException {
        ipBans.clear();
        nameBans.clear();
        uuidToIps.clear();

        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM bans WHERE expiry_timestamp = -1 OR expiry_timestamp > ?")) {
            ps.setLong(1, System.currentTimeMillis());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                BanEntry entry = readEntry(rs);
                cacheEntry(entry);
            }
        }
    }

    private void cacheEntry(BanEntry entry) {
        if (entry.isIpBan()) {
            ipBans.put(entry.getValue(), entry);
            if (entry.getPlayerUuid() != null && !entry.getPlayerUuid().isEmpty()) {
                uuidToIps.computeIfAbsent(entry.getPlayerUuid(), k -> ConcurrentHashMap.newKeySet())
                        .add(entry.getValue());
            }
        } else {
            nameBans.put(entry.getValue().toLowerCase(), entry);
        }
    }

    private void uncacheEntry(BanEntry entry) {
        if (entry.isIpBan()) {
            ipBans.remove(entry.getValue());
            if (entry.getPlayerUuid() != null) {
                Set<String> ips = uuidToIps.get(entry.getPlayerUuid());
                if (ips != null) {
                    ips.remove(entry.getValue());
                    if (ips.isEmpty()) uuidToIps.remove(entry.getPlayerUuid());
                }
            }
        } else {
            nameBans.remove(entry.getValue().toLowerCase());
        }
    }

    private void cleanExpired() {
        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM bans WHERE expiry_timestamp != -1 AND expiry_timestamp <= ?")) {
            ps.setLong(1, System.currentTimeMillis());
            int removed = ps.executeUpdate();
            if (removed > 0) logger.info("[Field] Cleaned {} expired bans.", removed);
        } catch (SQLException e) {
            logger.error("[Field] Failed to clean expired bans", e);
        }
    }

    public void refreshCache() {
        try {
            cleanExpired();
            loadCache();
        } catch (SQLException e) {
            logger.error("[Field] Failed to refresh cache", e);
        }
    }

    // ===================== Add/Remove =====================

    public synchronized boolean addBan(BanType type, String value, String playerName,
                                       String playerUuid, long expiryTimestamp, String bannedBy) {
        String key = type == BanType.IP ? value : value.toLowerCase();

        // Check existing
        BanEntry existing = type == BanType.IP ? ipBans.get(key) : nameBans.get(key);
        if (existing != null && !existing.isExpired()) {
            return false;
        }

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT OR REPLACE INTO bans (type, value, player_name, player_uuid, ban_timestamp, expiry_timestamp, banned_by) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, type.name());
            ps.setString(2, type == BanType.NAME ? value.toLowerCase() : value);
            ps.setString(3, playerName);
            ps.setString(4, playerUuid);
            ps.setLong(5, System.currentTimeMillis());
            ps.setLong(6, expiryTimestamp);
            ps.setString(7, bannedBy);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("[Field] Failed to add ban: {} {}", type, value, e);
            return false;
        }

        // Reload entry
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM bans WHERE type = ? AND value = ?")) {
            ps.setString(1, type.name());
            ps.setString(2, type == BanType.NAME ? value.toLowerCase() : value);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                cacheEntry(readEntry(rs));
            }
        } catch (SQLException e) {
            logger.error("[Field] Failed to reload ban entry", e);
        }

        return true;
    }

    public synchronized boolean removeBan(BanType type, String value) {
        String key = type == BanType.IP ? value : value.toLowerCase();

        BanEntry existing = type == BanType.IP ? ipBans.get(key) : nameBans.get(key);
        if (existing != null) uncacheEntry(existing);

        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM bans WHERE type = ? AND value = ?")) {
            ps.setString(1, type.name());
            ps.setString(2, key);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            logger.error("[Field] Failed to remove ban: {} {}", type, value, e);
            return false;
        }
    }

    /**
     * Clear ALL bans. Returns count removed.
     */
    public synchronized int clearAllBans() {
        int count = ipBans.size() + nameBans.size();
        ipBans.clear();
        nameBans.clear();
        uuidToIps.clear();

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM bans");
        } catch (SQLException e) {
            logger.error("[Field] Failed to clear bans", e);
        }

        return count;
    }

    // ===================== Query =====================

    public boolean isIpBanned(String ip) {
        BanEntry entry = ipBans.get(ip);
        if (entry == null) return false;
        if (entry.isExpired()) {
            removeBan(BanType.IP, ip);
            return false;
        }
        return true;
    }

    public boolean isNameBanned(String name) {
        BanEntry entry = nameBans.get(name.toLowerCase());
        if (entry == null) return false;
        if (entry.isExpired()) {
            removeBan(BanType.NAME, name.toLowerCase());
            return false;
        }
        return true;
    }

    public Optional<BanEntry> getIpBan(String ip) {
        BanEntry entry = ipBans.get(ip);
        if (entry == null) return Optional.empty();
        if (entry.isExpired()) { removeBan(BanType.IP, ip); return Optional.empty(); }
        return Optional.of(entry);
    }

    public Optional<BanEntry> getNameBan(String name) {
        BanEntry entry = nameBans.get(name.toLowerCase());
        if (entry == null) return Optional.empty();
        if (entry.isExpired()) { removeBan(BanType.NAME, name.toLowerCase()); return Optional.empty(); }
        return Optional.of(entry);
    }

    public boolean isPlayerUuidBanned(String uuid) {
        if (uuid == null) return false;
        Set<String> ips = uuidToIps.get(uuid);
        if (ips == null || ips.isEmpty()) return false;
        for (String ip : ips) {
            if (isIpBanned(ip)) return true;
        }
        return false;
    }

    public Set<String> getBannedIpsForUuid(String uuid) {
        if (uuid == null) return Set.of();
        Set<String> ips = uuidToIps.get(uuid);
        if (ips == null) return Set.of();
        Set<String> active = new HashSet<>();
        for (String ip : ips) {
            if (isIpBanned(ip)) active.add(ip);
        }
        return active;
    }

    /**
     * Check if an IP or a name is banned (unified check).
     */
    public boolean isBanned(String ipOrName) {
        return isIpBanned(ipOrName) || isNameBanned(ipOrName);
    }

    // ===================== Listing =====================

    public List<BanEntry> getAllActiveBans() {
        List<BanEntry> list = new ArrayList<>();
        for (BanEntry e : ipBans.values()) {
            if (!e.isExpired()) list.add(e);
        }
        for (BanEntry e : nameBans.values()) {
            if (!e.isExpired()) list.add(e);
        }
        list.sort(Comparator.comparingLong(BanEntry::getBanTimestamp).reversed());
        return list;
    }

    public List<BanEntry> getActiveBansPaginated(int page, int pageSize) {
        List<BanEntry> all = getAllActiveBans();
        int start = page * pageSize;
        if (start >= all.size()) return List.of();
        return all.subList(start, Math.min(start + pageSize, all.size()));
    }

    public int getActiveBanCount() {
        int count = 0;
        for (BanEntry e : ipBans.values()) if (!e.isExpired()) count++;
        for (BanEntry e : nameBans.values()) if (!e.isExpired()) count++;
        return count;
    }

    private BanEntry readEntry(ResultSet rs) throws SQLException {
        return new BanEntry(
                rs.getInt("id"),
                BanType.valueOf(rs.getString("type")),
                rs.getString("value"),
                rs.getString("player_name"),
                rs.getString("player_uuid"),
                rs.getLong("ban_timestamp"),
                rs.getLong("expiry_timestamp"),
                rs.getString("banned_by")
        );
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) connection.close();
        } catch (SQLException e) {
            logger.error("[Field] Failed to close database", e);
        }
    }
}