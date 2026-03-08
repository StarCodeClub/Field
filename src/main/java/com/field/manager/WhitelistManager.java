package com.field.manager;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the vanish-mode IP whitelist.
 * Whitelisted IPs can connect and ping even when vanish is enabled.
 */
public class WhitelistManager {

    private final Path whitelistFile;
    private final Logger logger;
    private final Set<String> whitelist = ConcurrentHashMap.newKeySet();

    public WhitelistManager(Path dataDirectory, Logger logger) {
        this.whitelistFile = dataDirectory.resolve("whitelist.txt");
        this.logger = logger;
    }

    public void load() {
        whitelist.clear();

        try {
            if (!Files.exists(whitelistFile)) {
                String defaultContent = """
                        # Field Vanish Whitelist
                        # One IP per line. These IPs can connect even when vanish mode is on.
                        # Lines starting with # are comments.
                        """;
                Files.writeString(whitelistFile, defaultContent, StandardCharsets.UTF_8);
                logger.info("[Field] Created default whitelist.txt");
                return;
            }

            List<String> lines = Files.readAllLines(whitelistFile, StandardCharsets.UTF_8);
            for (String line : lines) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
                whitelist.add(trimmed);
            }

            logger.info("[Field] Whitelist loaded: {} IPs", whitelist.size());

        } catch (IOException e) {
            logger.error("[Field] Failed to load whitelist", e);
        }
    }

    public void save() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("# Field Vanish Whitelist\n");
            sb.append("# One IP per line. These IPs can connect even when vanish mode is on.\n");
            sb.append("# Lines starting with # are comments.\n\n");
            for (String ip : whitelist) {
                sb.append(ip).append("\n");
            }
            Files.writeString(whitelistFile, sb.toString(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("[Field] Failed to save whitelist", e);
        }
    }

    public boolean isWhitelisted(String ip) {
        if (ip == null) return false;
        return whitelist.contains(ip);
    }

    public boolean add(String ip) {
        boolean added = whitelist.add(ip);
        if (added) save();
        return added;
    }

    public boolean remove(String ip) {
        boolean removed = whitelist.remove(ip);
        if (removed) save();
        return removed;
    }

    public Set<String> getAll() {
        return Set.copyOf(whitelist);
    }

    public int size() {
        return whitelist.size();
    }
}