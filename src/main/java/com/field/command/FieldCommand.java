package com.field.command;

import com.field.FieldPlugin;
import com.field.manager.BanManager;
import com.field.manager.ConnectionInterceptor;
import com.field.model.BanEntry;
import com.field.model.BanEntry.BanType;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.command.SimpleCommand;
import com.velocitypowered.api.proxy.ConsoleCommandSource;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ProxyServer;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.event.ClickEvent;
import net.kyori.adventure.text.event.HoverEvent;
import net.kyori.adventure.text.minimessage.MiniMessage;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FieldCommand implements SimpleCommand {

    private final FieldPlugin plugin;
    private final ProxyServer server;
    private final Logger logger;
    private final MiniMessage mm = MiniMessage.miniMessage();
    private static final int PAGE_SIZE = 8;

    public FieldCommand(FieldPlugin plugin, ProxyServer server, Logger logger) {
        this.plugin = plugin;
        this.server = server;
        this.logger = logger;
    }

    @Override
    public void execute(Invocation invocation) {
        CommandSource src = invocation.source();
        String[] args = invocation.arguments();

        if (args.length == 0) { sendHelp(src); return; }

        switch (args[0].toLowerCase()) {
            case "kick"      -> doKick(src, args);
            case "ban"       -> doBan(src, args);
            case "unban"     -> doUnban(src, args);
            case "banlist"   -> doBanList(src, args);
            case "vanish"    -> doVanish(src, args);
            case "whitelist" -> doWhitelist(src, args);
            case "reload"    -> doReload(src, args);
            case "status"    -> doStatus(src, args);
            case "help"      -> sendHelp(src);
            default -> { msg(src, "<red>Unknown subcommand: <white>" + args[0]); sendHelp(src); }
        }
    }

    // ============ KICK ============

    private void doKick(CommandSource src, String[] args) {
        if (!hasPerm(src, "field.kick")) { noPerms(src); return; }
        if (args.length < 2) {
            msg(src, "<yellow>Usage: <white>/field kick <player | IP>");
            return;
        }

        String target = args[1];

        if (BanManager.isIpAddress(target)) {
            int closed = plugin.getConnectionInterceptor().forceCloseByIp(target);
            for (Player p : server.getAllPlayers()) {
                if (target.equals(playerIp(p))) {
                    ConnectionInterceptor.forceDisconnectPlayer(p);
                    closed++;
                }
            }
            msg(src, plugin.getConfig().getMessage("kicked-ip",
                    Map.of("ip", target, "count", String.valueOf(closed))));
            logger.info("[Field] {} force-disconnected IP {} ({} connections)", srcName(src), target, closed);
        } else {
            Optional<Player> opt = server.getPlayer(target);
            if (opt.isEmpty()) {
                msg(src, plugin.getConfig().getMessage("player-not-found", Map.of("target", target)));
                return;
            }
            Player player = opt.get();
            String ip = playerIp(player);
            ConnectionInterceptor.forceDisconnectPlayer(player);
            if (ip != null) plugin.getConnectionInterceptor().forceCloseByIp(ip);
            msg(src, plugin.getConfig().getMessage("kicked-player", Map.of("player", player.getUsername())));
            logger.info("[Field] {} force-disconnected {} (IP: {})", srcName(src), player.getUsername(), ip);
        }
    }

    // ============ BAN ============

    private void doBan(CommandSource src, String[] args) {
        if (!hasPerm(src, "field.ban")) { noPerms(src); return; }
        if (args.length < 2) {
            msg(src, "<yellow>Usage: <white>/field ban <player/IP> [duration]");
            msg(src, "<gray>Duration: 30s, 5m, 2h, 7d, permanent (default)");
            msg(src, "<gray>Auto-detects IP vs player name.");
            return;
        }

        String target = args[1];
        String durationStr = args.length >= 3 ? args[2] : "permanent";
        long duration = BanManager.parseDuration(durationStr);

        if (BanManager.isIpAddress(target)) {
            // Direct IP ban
            if (plugin.getBanManager().isIpBanned(target)) {
                msg(src, plugin.getConfig().getMessage("already-banned", Map.of("target", target)));
                return;
            }

            // Try to find a player with this IP for association
            String assocName = null;
            String assocUuid = null;
            for (Player p : server.getAllPlayers()) {
                if (target.equals(playerIp(p))) {
                    assocName = p.getUsername();
                    assocUuid = p.getUniqueId().toString();
                    break;
                }
            }

            plugin.getBanManager().banIp(target, assocName, assocUuid, duration, srcName(src));
            msg(src, plugin.getConfig().getMessage("ip-banned", Map.of("ip", target)));
            String durDisplay = duration == -1 ? "permanent" : BanEntry.formatMillis(duration);
            msg(src, "<gray>Duration: <white>" + durDisplay);
            if (assocName != null) msg(src, "<gray>Player: <white>" + assocName);

            // Force disconnect
            plugin.getConnectionInterceptor().forceCloseByIp(target);
            for (Player p : server.getAllPlayers()) {
                if (target.equals(playerIp(p))) ConnectionInterceptor.forceDisconnectPlayer(p);
            }

            logger.info("[Field] {} banned IP {} (duration={})", srcName(src), target, durDisplay);

        } else {
            // Player name — check if online first
            Optional<Player> opt = server.getPlayer(target);

            if (opt.isPresent()) {
                // Online player: ban their IP
                Player player = opt.get();
                String ip = playerIp(player);
                if (ip == null) { msg(src, "<red>Cannot determine IP for " + target); return; }

                if (plugin.getBanManager().isIpBanned(ip)) {
                    msg(src, plugin.getConfig().getMessage("already-banned", Map.of("target", ip)));
                    return;
                }

                plugin.getBanManager().banIp(ip, player.getUsername(),
                        player.getUniqueId().toString(), duration, srcName(src));
                msg(src, plugin.getConfig().getMessage("ip-banned", Map.of("ip", ip)));
                String durDisplay = duration == -1 ? "permanent" : BanEntry.formatMillis(duration);
                msg(src, "<gray>Duration: <white>" + durDisplay);
                msg(src, "<gray>Player: <white>" + player.getUsername());

                ConnectionInterceptor.forceDisconnectPlayer(player);
                plugin.getConnectionInterceptor().forceCloseByIp(ip);

                logger.info("[Field] {} banned IP {} for player {} (duration={})",
                        srcName(src), ip, player.getUsername(), durDisplay);
            } else {
                // Offline player: ban the name
                if (plugin.getBanManager().isNameBanned(target)) {
                    msg(src, plugin.getConfig().getMessage("already-banned", Map.of("target", target)));
                    return;
                }

                plugin.getBanManager().banName(target, duration, srcName(src));
                msg(src, plugin.getConfig().getMessage("name-banned", Map.of("name", target)));
                String durDisplay = duration == -1 ? "permanent" : BanEntry.formatMillis(duration);
                msg(src, "<gray>Duration: <white>" + durDisplay);
                msg(src, "<gray>IP will be auto-banned when they try to connect.");

                logger.info("[Field] {} banned name '{}' (duration={})", srcName(src), target, durDisplay);
            }
        }
    }

    // ============ UNBAN ============

    private void doUnban(CommandSource src, String[] args) {
        if (!hasPerm(src, "field.ban")) { noPerms(src); return; }
        if (args.length < 2) {
            msg(src, "<yellow>Usage: <white>/field unban <IP | player name>");
            return;
        }

        String target = args[1];

        if (plugin.getBanManager().unban(target)) {
            if (BanManager.isIpAddress(target)) {
                msg(src, plugin.getConfig().getMessage("ip-unbanned", Map.of("ip", target)));
            } else {
                msg(src, plugin.getConfig().getMessage("name-unbanned", Map.of("name", target)));
            }
            logger.info("[Field] {} unbanned {}", srcName(src), target);
        } else {
            msg(src, plugin.getConfig().getMessage("not-banned", Map.of("target", target)));
        }
    }

    // ============ BANLIST ============

    private void doBanList(CommandSource src, String[] args) {
        if (!hasPerm(src, "field.banlist")) { noPerms(src); return; }

        // Check for "clear" subcommand
        if (args.length >= 2 && args[1].equalsIgnoreCase("clear")) {
            if (!hasPerm(src, "field.ban")) { noPerms(src); return; }
            int count = plugin.getBanManager().clearAll();
            msg(src, plugin.getConfig().getMessage("banlist-cleared",
                    Map.of("count", String.valueOf(count))));
            logger.info("[Field] {} cleared all bans ({} removed)", srcName(src), count);
            return;
        }

        int page = 0;
        if (args.length >= 2) {
            try { page = Math.max(0, Integer.parseInt(args[1]) - 1); }
            catch (NumberFormatException ignored) {}
        }

        int total = plugin.getBanManager().getTotalBans();
        int totalPages = Math.max(1, (int) Math.ceil((double) total / PAGE_SIZE));
        if (page >= totalPages) page = totalPages - 1;

        List<BanEntry> bans = plugin.getBanManager().getBansPaginated(page, PAGE_SIZE);

        msg(src, "<gold><bold>Ban List</bold> <gray>(<white>" + (page + 1)
                + "<gray>/<white>" + totalPages + "<gray>) - <white>" + total + " <gray>total"
                + " <dark_gray>| <red><click:run_command:'/field banlist clear'>[Clear All]</click>");

        if (bans.isEmpty()) {
            msg(src, "<gray>No active bans.");
            return;
        }

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault());

        for (BanEntry ban : bans) {
            String typeTag = ban.isNameBan() ? "<light_purple>[NAME]" : "<aqua>[IP]";
            String expiry = ban.isPermanent() ? "<red>永久" : "<yellow>" + ban.getFormattedRemaining();

            Component line = mm.deserialize(
                    plugin.getConfig().getPrefix()
                            + typeTag + " <white>" + ban.getDisplayLabel()
                            + " <dark_gray>| " + expiry);

            Component hover = mm.deserialize(
                    "<gold><bold>Ban Details</bold>\n"
                            + "<gray>Type: <white>" + ban.getType().name() + "\n"
                            + "<gray>Value: <white>" + ban.getValue() + "\n"
                            + (ban.getPlayerName() != null ?
                            "<gray>Player: <white>" + ban.getPlayerName() + "\n" : "")
                            + (ban.getPlayerUuid() != null ?
                            "<gray>UUID: <white>" + ban.getPlayerUuid() + "\n" : "")
                            + "<gray>Banned by: <white>" + ban.getBannedBy() + "\n"
                            + "<gray>Banned at: <white>" + dtf.format(Instant.ofEpochMilli(ban.getBanTimestamp())) + "\n"
                            + "<gray>Expires: <white>" + (ban.isPermanent() ? "Never"
                            : dtf.format(Instant.ofEpochMilli(ban.getExpiryTimestamp()))) + "\n"
                            + "\n<yellow>Click to unban");

            line = line.hoverEvent(HoverEvent.showText(hover))
                    .clickEvent(ClickEvent.suggestCommand("/field unban " + ban.getValue()));

            src.sendMessage(line);
        }

        if (totalPages > 1) {
            Component nav = Component.empty();
            if (page > 0) {
                nav = nav.append(mm.deserialize("<green>[← Prev]")
                        .clickEvent(ClickEvent.runCommand("/field banlist " + page)));
            }
            nav = nav.append(Component.text("  "));
            if (page < totalPages - 1) {
                nav = nav.append(mm.deserialize("<green>[Next →]")
                        .clickEvent(ClickEvent.runCommand("/field banlist " + (page + 2))));
            }
            src.sendMessage(nav);
        }
    }

    // ============ VANISH ============

    private void doVanish(CommandSource src, String[] args) {
        if (!hasPerm(src, "field.vanish")) { noPerms(src); return; }

        boolean newState;
        if (args.length >= 2) {
            String val = args[1].toLowerCase();
            newState = val.equals("on") || val.equals("enable") || val.equals("true");
            plugin.getVanishManager().setVanished(newState);
        } else {
            newState = plugin.getVanishManager().toggle();
        }

        if (newState) {
            // Force close all non-whitelisted connections
            int closed = 0;
            for (Player p : server.getAllPlayers()) {
                String ip = playerIp(p);
                if (ip == null || !plugin.getWhitelistManager().isWhitelisted(ip)) {
                    ConnectionInterceptor.forceDisconnectPlayer(p);
                    closed++;
                }
            }
            closed += plugin.getConnectionInterceptor().forceCloseAll();
            msg(src, plugin.getConfig().getMessage("vanish-enabled"));
            msg(src, "<gray>Whitelisted IPs (" + plugin.getWhitelistManager().size() + ") can still connect.");
            logger.info("[Field] {} enabled vanish. {} connections closed.", srcName(src), closed);
        } else {
            msg(src, plugin.getConfig().getMessage("vanish-disabled"));
            logger.info("[Field] {} disabled vanish.", srcName(src));
        }
    }

    // ============ WHITELIST ============

    private void doWhitelist(CommandSource src, String[] args) {
        if (!hasPerm(src, "field.vanish")) { noPerms(src); return; }

        if (args.length < 2) {
            msg(src, "<yellow>Usage:");
            msg(src, "<white>/field whitelist add <IP>");
            msg(src, "<white>/field whitelist remove <IP>");
            msg(src, "<white>/field whitelist list");
            msg(src, "<white>/field whitelist reload");
            return;
        }

        String sub = args[1].toLowerCase();

        switch (sub) {
            case "add" -> {
                if (args.length < 3) { msg(src, "<yellow>Usage: <white>/field whitelist add <IP>"); return; }
                String ip = args[2];
                plugin.getWhitelistManager().add(ip);
                msg(src, plugin.getConfig().getMessage("whitelist-added", Map.of("ip", ip)));
                logger.info("[Field] {} added {} to whitelist", srcName(src), ip);
            }
            case "remove", "rm", "del" -> {
                if (args.length < 3) { msg(src, "<yellow>Usage: <white>/field whitelist remove <IP>"); return; }
                String ip = args[2];
                if (plugin.getWhitelistManager().remove(ip)) {
                    msg(src, plugin.getConfig().getMessage("whitelist-removed", Map.of("ip", ip)));
                    logger.info("[Field] {} removed {} from whitelist", srcName(src), ip);
                } else {
                    msg(src, plugin.getConfig().getMessage("whitelist-not-found", Map.of("ip", ip)));
                }
            }
            case "list" -> {
                Set<String> all = plugin.getWhitelistManager().getAll();
                msg(src, "<gold><bold>Vanish Whitelist</bold> <gray>(" + all.size() + " IPs)");
                if (all.isEmpty()) {
                    msg(src, "<gray>No whitelisted IPs.");
                } else {
                    for (String ip : all) {
                        msg(src, "<white>  " + ip);
                    }
                }
            }
            case "reload" -> {
                plugin.getWhitelistManager().load();
                msg(src, "<green>Whitelist reloaded. " + plugin.getWhitelistManager().size() + " IPs loaded.");
            }
            default -> msg(src, "<red>Unknown whitelist subcommand: " + sub);
        }
    }

    // ============ RELOAD ============

    private void doReload(CommandSource src, String[] args) {
        if (!hasPerm(src, "field.reload")) { noPerms(src); return; }
        plugin.reload();
        msg(src, plugin.getConfig().getMessage("config-reloaded"));
        logger.info("[Field] {} reloaded config.", srcName(src));
    }

    // ============ STATUS ============

    private void doStatus(CommandSource src, String[] args) {
        msg(src, "<gold><bold>Field Status</bold>");
        msg(src, "<gray>Vanish: " +
                (plugin.getVanishManager().isVanished() ? "<red>ENABLED" : "<green>DISABLED"));
        msg(src, "<gray>Whitelist: <white>" + plugin.getWhitelistManager().size() + " <gray>IPs");
        msg(src, "<gray>Active Bans: <white>" + plugin.getBanManager().getTotalBans()
                + " <gray>(IP + Name)");
        msg(src, "<gray>Online Players: <white>" + server.getPlayerCount());
        msg(src, "<gray>TCP Connections: <white>" +
                plugin.getConnectionInterceptor().getTotalConnectionCount());
        msg(src, "<gray>Tracked IPs: <white>" +
                plugin.getConnectionInterceptor().getActiveChannels().size());
        msg(src, "<gray>Netty Interceptor: " +
                (plugin.getConnectionInterceptor().isInjected() ? "<green>ACTIVE" : "<red>INACTIVE"));
        msg(src, "<gray>Rate Limit: <white>" +
                plugin.getConfig().getMaxConnectionsPerSecond() + "/s <gray>(action: "
                + plugin.getConfig().getExceedAction() + ")");
    }

    // ============ HELP ============

    private void sendHelp(CommandSource src) {
        msg(src, "<gold><bold>Field</bold> <gray>- TCP-level connection control <dark_gray>v1.1.0");
        msg(src, "");
        msg(src, "<white>/field kick <player|IP> <gray>- Force TCP disconnect");
        msg(src, "<white>/field ban <player|IP> [duration] <gray>- Ban (auto-detects type)");
        msg(src, "<white>/field unban <player|IP> <gray>- Unban");
        msg(src, "<white>/field banlist [page|clear] <gray>- View/clear bans");
        msg(src, "<white>/field vanish [on|off] <gray>- Server vanish mode");
        msg(src, "<white>/field whitelist <add|remove|list|reload> <gray>- Vanish whitelist");
        msg(src, "<white>/field reload <gray>- Reload config & whitelist");
        msg(src, "<white>/field status <gray>- Plugin status & stats");
    }

    // ============ TAB COMPLETE ============

    @Override
    public CompletableFuture<List<String>> suggestAsync(Invocation invocation) {
        String[] args = invocation.arguments();
        CommandSource src = invocation.source();

        if (args.length <= 1) {
            String partial = args.length == 1 ? args[0].toLowerCase() : "";
            List<String> subs = new ArrayList<>();
            if (hasPerm(src, "field.kick"))    subs.add("kick");
            if (hasPerm(src, "field.ban"))     { subs.add("ban"); subs.add("unban"); }
            if (hasPerm(src, "field.banlist")) subs.add("banlist");
            if (hasPerm(src, "field.vanish"))  { subs.add("vanish"); subs.add("whitelist"); }
            if (hasPerm(src, "field.reload"))  subs.add("reload");
            subs.add("status"); subs.add("help");
            return CompletableFuture.completedFuture(
                    subs.stream().filter(s -> s.startsWith(partial)).collect(Collectors.toList()));
        }

        String sub = args[0].toLowerCase();
        String partial = args[args.length - 1].toLowerCase();

        return CompletableFuture.completedFuture(switch (sub) {
            case "kick" -> {
                if (args.length == 2) {
                    List<String> list = new ArrayList<>();
                    server.getAllPlayers().forEach(p -> list.add(p.getUsername()));
                    yield list.stream().filter(s -> s.toLowerCase().startsWith(partial))
                            .collect(Collectors.toList());
                }
                yield List.<String>of();
            }
            case "ban" -> {
                if (args.length == 2) {
                    List<String> list = new ArrayList<>();
                    server.getAllPlayers().forEach(p -> list.add(p.getUsername()));
                    yield list.stream().filter(s -> s.toLowerCase().startsWith(partial))
                            .collect(Collectors.toList());
                } else if (args.length == 3) {
                    yield Stream.of("permanent", "30s", "5m", "30m", "1h", "6h", "12h", "1d", "7d", "30d")
                            .filter(s -> s.startsWith(partial)).collect(Collectors.toList());
                }
                yield List.<String>of();
            }
            case "unban" -> {
                if (args.length == 2) {
                    List<String> list = new ArrayList<>();
                    plugin.getBanManager().getAllActiveBans().forEach(b -> list.add(b.getValue()));
                    yield list.stream().filter(s -> s.toLowerCase().startsWith(partial))
                            .collect(Collectors.toList());
                }
                yield List.<String>of();
            }
            case "banlist" -> {
                if (args.length == 2) {
                    yield Stream.of("clear", "1", "2", "3")
                            .filter(s -> s.startsWith(partial)).collect(Collectors.toList());
                }
                yield List.<String>of();
            }
            case "vanish" -> {
                if (args.length == 2) {
                    yield Stream.of("on", "off")
                            .filter(s -> s.startsWith(partial)).collect(Collectors.toList());
                }
                yield List.<String>of();
            }
            case "whitelist" -> {
                if (args.length == 2) {
                    yield Stream.of("add", "remove", "list", "reload")
                            .filter(s -> s.startsWith(partial)).collect(Collectors.toList());
                }
                if (args.length == 3 && (args[1].equalsIgnoreCase("remove")
                        || args[1].equalsIgnoreCase("rm")
                        || args[1].equalsIgnoreCase("del"))) {
                    yield plugin.getWhitelistManager().getAll().stream()
                            .filter(s -> s.startsWith(partial)).collect(Collectors.toList());
                }
                yield List.<String>of();
            }
            default -> List.<String>of();
        });
    }

    @Override
    public boolean hasPermission(Invocation invocation) {
        CommandSource src = invocation.source();
        return src instanceof ConsoleCommandSource
                || src.hasPermission("field.admin")
                || src.hasPermission("field.kick")
                || src.hasPermission("field.ban")
                || src.hasPermission("field.banlist")
                || src.hasPermission("field.vanish")
                || src.hasPermission("field.reload");
    }

    // ============ Helpers ============

    private boolean hasPerm(CommandSource src, String perm) {
        return src instanceof ConsoleCommandSource
                || src.hasPermission("field.admin")
                || src.hasPermission(perm);
    }

    private void noPerms(CommandSource src) {
        msg(src, plugin.getConfig().getMessage("no-permission"));
    }

    private void msg(CommandSource src, String text) {
        src.sendMessage(mm.deserialize(plugin.getConfig().getPrefix() + text));
    }

    private String srcName(CommandSource src) {
        return src instanceof Player p ? p.getUsername() : "Console";
    }

    private String playerIp(Player player) {
        try {
            InetSocketAddress addr = player.getRemoteAddress();
            if (addr != null && addr.getAddress() != null) return addr.getAddress().getHostAddress();
        } catch (Exception ignored) {}
        return null;
    }
}