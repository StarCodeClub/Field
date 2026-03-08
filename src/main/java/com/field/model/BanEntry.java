package com.field.model;

public class BanEntry {

    public enum BanType {
        IP,
        NAME
    }

    private final int id;
    private final BanType type;
    private final String value; // IP address or player name
    private final String playerName; // associated player name (for IP bans via player)
    private final String playerUuid; // associated UUID
    private final long banTimestamp;
    private final long expiryTimestamp; // -1 = permanent
    private final String bannedBy;

    public BanEntry(int id, BanType type, String value, String playerName,
                    String playerUuid, long banTimestamp, long expiryTimestamp, String bannedBy) {
        this.id = id;
        this.type = type;
        this.value = value;
        this.playerName = playerName;
        this.playerUuid = playerUuid;
        this.banTimestamp = banTimestamp;
        this.expiryTimestamp = expiryTimestamp;
        this.bannedBy = bannedBy;
    }

    public int getId() { return id; }
    public BanType getType() { return type; }
    public String getValue() { return value; }
    public String getPlayerName() { return playerName; }
    public String getPlayerUuid() { return playerUuid; }
    public long getBanTimestamp() { return banTimestamp; }
    public long getExpiryTimestamp() { return expiryTimestamp; }
    public String getBannedBy() { return bannedBy; }

    public boolean isPermanent() { return expiryTimestamp == -1; }

    public boolean isExpired() {
        return !isPermanent() && System.currentTimeMillis() > expiryTimestamp;
    }

    public boolean isIpBan() { return type == BanType.IP; }
    public boolean isNameBan() { return type == BanType.NAME; }

    public String getFormattedRemaining() {
        if (isPermanent()) return "永久";
        long remaining = expiryTimestamp - System.currentTimeMillis();
        if (remaining <= 0) return "已过期";
        return formatMillis(remaining);
    }

    /**
     * Display label for banlist.
     */
    public String getDisplayLabel() {
        if (type == BanType.NAME) {
            return value + " <gray>(name)";
        }
        // IP ban
        if (playerName != null && !playerName.isEmpty()) {
            return value + " <gray>(" + playerName + ")";
        }
        return value;
    }

    public static String formatMillis(long millis) {
        long s = millis / 1000;
        long m = s / 60; s %= 60;
        long h = m / 60; m %= 60;
        long d = h / 24; h %= 24;
        StringBuilder sb = new StringBuilder();
        if (d > 0) sb.append(d).append("d ");
        if (h > 0) sb.append(h).append("h ");
        if (m > 0) sb.append(m).append("m ");
        if (sb.length() == 0 || s > 0) sb.append(s).append("s");
        return sb.toString().trim();
    }
}