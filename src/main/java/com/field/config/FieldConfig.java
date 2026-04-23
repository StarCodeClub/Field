package com.field.config;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldConfig {

    private final Path dataDirectory;
    private final Logger logger;
    private final Path configFile;

    private volatile String prefix;
    private volatile boolean vanishOnStartup;
    private volatile boolean logBlockedConnections;
    private volatile int maxOnlinePerIp;
    private volatile int maxConnectionsPerSecond;
    private volatile String exceedAction; // "close" or "ban"
    private volatile String autoBanDuration;
    private volatile String databaseFile;
    private final ConcurrentHashMap<String, String> messages = new ConcurrentHashMap<>();

    private static final Pattern KV_PATTERN =
            Pattern.compile("^\\s*([\\w\\-]+)\\s*=\\s*\"(.*)\"\\s*$");
    private static final Pattern KV_BOOL_PATTERN =
            Pattern.compile("^\\s*([\\w\\-]+)\\s*=\\s*(true|false)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern KV_INT_PATTERN =
            Pattern.compile("^\\s*([\\w\\-]+)\\s*=\\s*(\\d+)\\s*$");
    private static final Pattern SECTION_PATTERN =
            Pattern.compile("^\\s*\\[([\\w\\-.]+)]\\s*$");

    public FieldConfig(Path dataDirectory, Logger logger) {
        this.dataDirectory = dataDirectory;
        this.logger = logger;
        this.configFile = dataDirectory.resolve("config.toml");
    }

    public void load() {
        try {
            if (!Files.exists(dataDirectory)) {
                Files.createDirectories(dataDirectory);
            }
            if (!Files.exists(configFile)) {
                saveDefault();
            }

            Map<String, String> flat = parseToml(configFile);

            this.prefix = flat.getOrDefault("general.prefix",
                    "<gradient:#ff6b6b:#ee5a24>Field</gradient> <dark_gray>» <reset>");
            this.vanishOnStartup = Boolean.parseBoolean(
                    flat.getOrDefault("general.vanish-on-startup", "false"));
            this.logBlockedConnections = Boolean.parseBoolean(
                    flat.getOrDefault("general.log-blocked-connections", "true"));
            try {
                this.maxOnlinePerIp = Integer.parseInt(
                        flat.getOrDefault("general.max-online-per-ip", "0"));
            } catch (NumberFormatException e) {
                this.maxOnlinePerIp = 0;
            }

            // Rate limit
            try {
                this.maxConnectionsPerSecond = Integer.parseInt(
                        flat.getOrDefault("rate-limit.max-connections-per-second", "4"));
            } catch (NumberFormatException e) {
                this.maxConnectionsPerSecond = 4;
            }
            this.exceedAction = flat.getOrDefault("rate-limit.exceed-action", "close");
            this.autoBanDuration = flat.getOrDefault("rate-limit.auto-ban-duration", "5m");

            this.databaseFile = flat.getOrDefault("database.file", "bans.db");

            messages.clear();
            for (Map.Entry<String, String> e : flat.entrySet()) {
                if (e.getKey().startsWith("messages.")) {
                    messages.put(e.getKey().substring("messages.".length()), e.getValue());
                }
            }
            ensureDefaultMessages();

            logger.info("[Field] Configuration loaded.");
        } catch (Exception e) {
            logger.error("[Field] Failed to load config, using defaults", e);
            loadDefaults();
        }
    }

    private Map<String, String> parseToml(Path file) throws IOException {
        Map<String, String> result = new HashMap<>();
        String currentSection = "";

        for (String rawLine : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            Matcher secMatch = SECTION_PATTERN.matcher(line);
            if (secMatch.matches()) {
                currentSection = secMatch.group(1);
                continue;
            }

            Matcher kvMatch = KV_PATTERN.matcher(line);
            if (kvMatch.matches()) {
                String key = currentSection.isEmpty()
                        ? kvMatch.group(1) : currentSection + "." + kvMatch.group(1);
                result.put(key, kvMatch.group(2));
                continue;
            }

            Matcher boolMatch = KV_BOOL_PATTERN.matcher(line);
            if (boolMatch.matches()) {
                String key = currentSection.isEmpty()
                        ? boolMatch.group(1) : currentSection + "." + boolMatch.group(1);
                result.put(key, boolMatch.group(2).toLowerCase());
                continue;
            }

            Matcher intMatch = KV_INT_PATTERN.matcher(line);
            if (intMatch.matches()) {
                String key = currentSection.isEmpty()
                        ? intMatch.group(1) : currentSection + "." + intMatch.group(1);
                result.put(key, intMatch.group(2));
            }
        }
        return result;
    }

    private void ensureDefaultMessages() {
        messages.putIfAbsent("no-permission", "<red>You do not have permission to use this command.");
        messages.putIfAbsent("player-not-found", "<red>Target not found or not online: <white>{target}");
        messages.putIfAbsent("ip-banned", "<green>Banned IP: <white>{ip}");
        messages.putIfAbsent("ip-unbanned", "<green>Unbanned IP: <white>{ip}");
        messages.putIfAbsent("name-banned", "<green>Banned player name: <white>{name}");
        messages.putIfAbsent("name-unbanned", "<green>Unbanned player name: <white>{name}");
        messages.putIfAbsent("not-banned", "<yellow><white>{target}</white> is not banned.");
        messages.putIfAbsent("vanish-enabled", "<green>Vanish mode <white>enabled</white>. All connections dropped.");
        messages.putIfAbsent("vanish-disabled", "<green>Vanish mode <white>disabled</white>. Server accepting connections.");
        messages.putIfAbsent("kicked-player", "<green>Force-disconnected player <white>{player}</white>.");
        messages.putIfAbsent("kicked-ip", "<green>Force-disconnected IP <white>{ip}</white>. ({count} closed)");
        messages.putIfAbsent("config-reloaded", "<green>Configuration and whitelist reloaded.");
        messages.putIfAbsent("already-banned", "<yellow><white>{target}</white> is already banned.");
        messages.putIfAbsent("banlist-cleared", "<green>All bans cleared. ({count} removed)");
        messages.putIfAbsent("rate-limited", "<yellow>Rate limited IP: <white>{ip}</white> ({count}/s)");
        messages.putIfAbsent("whitelist-added", "<green>Added <white>{ip}</white> to vanish whitelist.");
        messages.putIfAbsent("whitelist-removed", "<green>Removed <white>{ip}</white> from vanish whitelist.");
        messages.putIfAbsent("whitelist-not-found", "<yellow><white>{ip}</white> is not in the whitelist.");
    }

    private void loadDefaults() {
        this.prefix = "<gradient:#ff6b6b:#ee5a24>Field</gradient> <dark_gray>» <reset>";
        this.vanishOnStartup = false;
        this.logBlockedConnections = true;
        this.maxOnlinePerIp = 0;
        this.maxConnectionsPerSecond = 4;
        this.exceedAction = "close";
        this.autoBanDuration = "5m";
        this.databaseFile = "bans.db";
        messages.clear();
        ensureDefaultMessages();
    }

    private void saveDefault() throws IOException {
        String content = """
                # Field Plugin Configuration
                
                [general]
                prefix = "<gradient:#ff6b6b:#ee5a24>Field</gradient> <dark_gray>» <reset>"
                vanish-on-startup = false
                log-blocked-connections = true
                # Max online players with the same IP. 0 = disabled
                max-online-per-ip = 0
                
                [rate-limit]
                # Max new TCP connections per IP per second. 0 = disabled
                max-connections-per-second = 4
                # Action when exceeded: "close" or "ban"
                exceed-action = "close"
                # Auto-ban duration when action is "ban"
                auto-ban-duration = "5m"
                
                [messages]
                no-permission = "<red>You do not have permission to use this command."
                player-not-found = "<red>Target not found or not online: <white>{target}"
                ip-banned = "<green>Banned IP: <white>{ip}"
                ip-unbanned = "<green>Unbanned IP: <white>{ip}"
                name-banned = "<green>Banned player name: <white>{name}"
                name-unbanned = "<green>Unbanned player name: <white>{name}"
                not-banned = "<yellow><white>{target}</white> is not banned."
                vanish-enabled = "<green>Vanish mode <white>enabled</white>. All connections dropped."
                vanish-disabled = "<green>Vanish mode <white>disabled</white>. Server accepting connections."
                kicked-player = "<green>Force-disconnected player <white>{player}</white>."
                kicked-ip = "<green>Force-disconnected IP <white>{ip}</white>. ({count} closed)"
                config-reloaded = "<green>Configuration and whitelist reloaded."
                already-banned = "<yellow><white>{target}</white> is already banned."
                banlist-cleared = "<green>All bans cleared. ({count} removed)"
                rate-limited = "<yellow>Rate limited IP: <white>{ip}</white> ({count}/s)"
                whitelist-added = "<green>Added <white>{ip}</white> to vanish whitelist."
                whitelist-removed = "<green>Removed <white>{ip}</white> from vanish whitelist."
                whitelist-not-found = "<yellow><white>{ip}</white> is not in the whitelist."
                
                [database]
                file = "bans.db"
                """;
        Files.writeString(configFile, content, StandardCharsets.UTF_8);
    }

    public String getPrefix() { return prefix; }
    public boolean isVanishOnStartup() { return vanishOnStartup; }
    public boolean isLogBlockedConnections() { return logBlockedConnections; }
    public int getMaxOnlinePerIp() { return maxOnlinePerIp; }
    public int getMaxConnectionsPerSecond() { return maxConnectionsPerSecond; }
    public String getExceedAction() { return exceedAction; }
    public String getAutoBanDuration() { return autoBanDuration; }
    public String getDatabaseFile() { return databaseFile; }

    public String getMessage(String key) {
        return messages.getOrDefault(key, "<red>Missing message: " + key);
    }

    public String getMessage(String key, Map<String, String> placeholders) {
        String msg = getMessage(key);
        for (Map.Entry<String, String> e : placeholders.entrySet()) {
            msg = msg.replace("{" + e.getKey() + "}", e.getValue());
        }
        return msg;
    }
}
