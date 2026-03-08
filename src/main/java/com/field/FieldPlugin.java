package com.field;

import com.field.command.FieldCommand;
import com.field.config.FieldConfig;
import com.field.database.BanDatabase;
import com.field.listener.ConnectionListener;
import com.field.listener.LoginListener;
import com.field.manager.BanManager;
import com.field.manager.ConnectionInterceptor;
import com.field.manager.VanishManager;
import com.field.manager.WhitelistManager;
import com.google.inject.Inject;
import com.velocitypowered.api.command.CommandMeta;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.ProxyServer;
import org.slf4j.Logger;

import java.nio.file.Path;

@Plugin(
        id = "field",
        name = "Field",
        version = "1.1.0",
        description = "Advanced TCP-level connection control for Velocity",
        authors = {"xiaomu18"}
)
public class FieldPlugin {

    private final ProxyServer server;
    private final Logger logger;
    private final Path dataDirectory;

    private FieldConfig config;
    private BanDatabase database;
    private BanManager banManager;
    private VanishManager vanishManager;
    private WhitelistManager whitelistManager;
    private ConnectionInterceptor connectionInterceptor;

    @Inject
    public FieldPlugin(ProxyServer server, Logger logger, @DataDirectory Path dataDirectory) {
        this.server = server;
        this.logger = logger;
        this.dataDirectory = dataDirectory;
    }

    @Subscribe
    public void onProxyInitialize(ProxyInitializeEvent event) {
        logger.info("[Field] Initializing...");

        // Config
        this.config = new FieldConfig(dataDirectory, logger);
        this.config.load();

        // Whitelist
        this.whitelistManager = new WhitelistManager(dataDirectory, logger);
        this.whitelistManager.load();

        // Database
        this.database = new BanDatabase(dataDirectory, config, logger);
        this.database.initialize();

        // Managers
        this.banManager = new BanManager(database, logger);
        this.vanishManager = new VanishManager(logger);

        // Netty interceptor
        this.connectionInterceptor = new ConnectionInterceptor(this, server, logger);
        this.connectionInterceptor.inject();

        // Vanish on startup
        if (config.isVanishOnStartup()) {
            vanishManager.setVanished(true);
            logger.info("[Field] Vanish mode enabled on startup.");
        }

        // Listeners
        server.getEventManager().register(this, new ConnectionListener(this, server, logger));
        server.getEventManager().register(this, new LoginListener(this, server, logger));

        // Command
        FieldCommand cmd = new FieldCommand(this, server, logger);
        CommandMeta meta = server.getCommandManager().metaBuilder("field")
                .aliases("fd")
                .plugin(this)
                .build();
        server.getCommandManager().register(meta, cmd);

        logger.info("[Field] Initialized successfully.");
    }

    @Subscribe
    public void onProxyShutdown(ProxyShutdownEvent event) {
        logger.info("[Field] Shutting down...");
        if (connectionInterceptor != null) connectionInterceptor.uninject();
        if (database != null) database.close();
        logger.info("[Field] Shut down.");
    }

    public void reload() {
        config.load();
        whitelistManager.load();
        banManager.refreshCache();
        logger.info("[Field] Configuration, whitelist, and bans reloaded.");
    }

    public ProxyServer getServer() { return server; }
    public Logger getLogger() { return logger; }
    public Path getDataDirectory() { return dataDirectory; }
    public FieldConfig getConfig() { return config; }
    public BanDatabase getDatabase() { return database; }
    public BanManager getBanManager() { return banManager; }
    public VanishManager getVanishManager() { return vanishManager; }
    public WhitelistManager getWhitelistManager() { return whitelistManager; }
    public ConnectionInterceptor getConnectionInterceptor() { return connectionInterceptor; }
}