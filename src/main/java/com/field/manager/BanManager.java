package com.field.manager;

import com.field.database.BanDatabase;
import com.field.model.BanEntry;
import com.field.model.BanEntry.BanType;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class BanManager {

    private final BanDatabase database;
    private final Logger logger;

    // Simple IP pattern: digits and dots (v4) or hex and colons (v6)
    private static final Pattern IP_V4 = Pattern.compile("^\\d{1,3}(\\.\\d{1,3}){3}$");
    private static final Pattern IP_V6 = Pattern.compile("^[0-9a-fA-F:]+$");

    public BanManager(BanDatabase database, Logger logger) {
        this.database = database;
        this.logger = logger;
    }

    /**
     * Auto-detect if the target is an IP or a player name.
     */
    public static boolean isIpAddress(String input) {
        if (input == null || input.isEmpty()) return false;
        return IP_V4.matcher(input).matches()
                || (input.contains(":") && IP_V6.matcher(input).matches());
    }

    // ===================== Ban operations =====================

    public boolean banIp(String ip, String playerName, String playerUuid,
                         long durationMillis, String bannedBy) {
        long expiry = durationMillis == -1 ? -1 : System.currentTimeMillis() + durationMillis;
        return database.addBan(BanType.IP, ip, playerName, playerUuid, expiry, bannedBy);
    }

    public boolean banName(String name, long durationMillis, String bannedBy) {
        long expiry = durationMillis == -1 ? -1 : System.currentTimeMillis() + durationMillis;
        return database.addBan(BanType.NAME, name.toLowerCase(), null, null, expiry, bannedBy);
    }

    /**
     * Auto-detect and ban. Returns the BanType used.
     */
    public BanType autoBan(String target, String playerName, String playerUuid,
                           long durationMillis, String bannedBy) {
        if (isIpAddress(target)) {
            banIp(target, playerName, playerUuid, durationMillis, bannedBy);
            return BanType.IP;
        } else {
            banName(target, durationMillis, bannedBy);
            return BanType.NAME;
        }
    }

    public boolean unban(String target) {
        if (isIpAddress(target)) {
            return database.removeBan(BanType.IP, target);
        } else {
            return database.removeBan(BanType.NAME, target.toLowerCase());
        }
    }

    public int clearAll() {
        return database.clearAllBans();
    }

    // ===================== Query =====================

    public boolean isIpBanned(String ip) { return database.isIpBanned(ip); }
    public boolean isNameBanned(String name) { return database.isNameBanned(name); }
    public boolean isPlayerUuidBanned(String uuid) { return database.isPlayerUuidBanned(uuid); }
    public Set<String> getBannedIpsForUuid(String uuid) { return database.getBannedIpsForUuid(uuid); }

    public boolean isBannedAny(String target) {
        return database.isBanned(target);
    }

    public Optional<BanEntry> getBan(String target) {
        if (isIpAddress(target)) return database.getIpBan(target);
        return database.getNameBan(target);
    }

    // ===================== Listing =====================

    public List<BanEntry> getAllActiveBans() { return database.getAllActiveBans(); }
    public List<BanEntry> getBansPaginated(int page, int pageSize) {
        return database.getActiveBansPaginated(page, pageSize);
    }
    public int getTotalBans() { return database.getActiveBanCount(); }
    public void refreshCache() { database.refreshCache(); }

    // ===================== Duration parsing =====================

    public static long parseDuration(String input) {
        if (input == null || input.isEmpty()
                || input.equalsIgnoreCase("permanent")
                || input.equalsIgnoreCase("perm")
                || input.equalsIgnoreCase("forever")
                || input.equals("-1")) {
            return -1;
        }

        long total = 0;
        StringBuilder numBuf = new StringBuilder();

        for (char c : input.toCharArray()) {
            if (Character.isDigit(c)) {
                numBuf.append(c);
            } else if (numBuf.length() > 0) {
                long num = Long.parseLong(numBuf.toString());
                numBuf.setLength(0);
                total += switch (Character.toLowerCase(c)) {
                    case 's' -> num * 1000L;
                    case 'm' -> num * 60_000L;
                    case 'h' -> num * 3_600_000L;
                    case 'd' -> num * 86_400_000L;
                    case 'w' -> num * 604_800_000L;
                    default -> 0L;
                };
            }
        }

        if (numBuf.length() > 0 && total == 0) {
            total = Long.parseLong(numBuf.toString()) * 1000L;
        }

        return total > 0 ? total : -1;
    }
}