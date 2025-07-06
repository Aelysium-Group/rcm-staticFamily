package group.aelysium.rustyconnector.modules.static_family;
import group.aelysium.rustyconnector.RC;
import group.aelysium.rustyconnector.common.errors.Error;
import group.aelysium.rustyconnector.common.haze.HazeDatabase;
import group.aelysium.rustyconnector.common.modules.Module;
import group.aelysium.rustyconnector.proxy.events.FamilyPreJoinEvent;
import group.aelysium.rustyconnector.proxy.family.Family;
import group.aelysium.rustyconnector.proxy.family.Server;
import group.aelysium.rustyconnector.proxy.family.load_balancing.LoadBalancer;
import group.aelysium.rustyconnector.proxy.player.Player;
import group.aelysium.rustyconnector.proxy.util.AddressUtil;
import group.aelysium.rustyconnector.proxy.util.LiquidTimestamp;
import group.aelysium.rustyconnector.shaded.group.aelysium.ara.Flux;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.lib.DataHolder;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.lib.Filter;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.lib.Filterable;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.lib.Type;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.requests.*;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.JoinConfiguration;
import net.kyori.adventure.text.format.NamedTextColor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static net.kyori.adventure.text.Component.*;
import static net.kyori.adventure.text.JoinConfiguration.newlines;
import static net.kyori.adventure.text.format.NamedTextColor.*;

public class StaticFamily extends Family {
    private static final String RESIDENCE_TABLE = "staticFamily_residence";

    protected final LiquidTimestamp residenceExpiration;
    protected final UnavailableProtocol unavailableProtocol;
    protected final StorageProtocol storageProtocol;
    protected final String databaseName;
    protected final Flux<HazeDatabase> database;

    public StaticFamily(
            @NotNull String id,
            @Nullable String displayName,
            @Nullable String parent,
            @NotNull Map<String, Object> metadata,
            @NotNull Module.Builder<LoadBalancer> loadBalancer,
            @NotNull LiquidTimestamp residenceExpiration,
            @NotNull UnavailableProtocol unavailableProtocol,
            @NotNull StorageProtocol storageProtocol,
            @NotNull String database
    ) throws Exception {
        super(id, displayName, parent, metadata);
        this.registerModule(loadBalancer);
        this.unavailableProtocol = unavailableProtocol;
        this.residenceExpiration = residenceExpiration;
        this.storageProtocol = storageProtocol;
        this.databaseName = database;

        this.database = RC.P.Haze().fetchDatabase(this.databaseName);
        if(this.database == null) throw new NoSuchElementException("No database exists on the haze provider with the name '"+this.databaseName+"'.");
        
        HazeDatabase db = this.database.get(15, TimeUnit.SECONDS);
        if(db.doesDataHolderExist(RESIDENCE_TABLE)) return;

        DataHolder table = new DataHolder(RESIDENCE_TABLE);
        table.addKey("player_uuid", Type.STRING(36).nullable(false));
        table.addKey("server_id", Type.STRING(64).nullable(false));
        table.addKey("family_id", Type.STRING(16).nullable(false));
        table.addKey("last_joined", Type.DATETIME().nullable(false));
        db.createDataHolder(table);
    }

    public @NotNull UnavailableProtocol unavailableProtocol() {
        return this.unavailableProtocol;
    }

    public @NotNull StorageProtocol storageProtocol() {
        return this.storageProtocol;
    }

    public @NotNull LiquidTimestamp residenceExpiration() {
        return this.residenceExpiration;
    }

    public @NotNull Flux<? extends HazeDatabase> database() {
        return this.database;
    }

    public Flux<? extends LoadBalancer> loadBalancer() {
        return this.fetchModule("LoadBalancer");
    }

    public void addServer(@NotNull Server server) {
        this.loadBalancer().ifPresent(l -> l.addServer(server));
    }

    public void removeServer(@NotNull Server server) {
        this.loadBalancer().ifPresent(l -> l.removeServer(server));
    }

    @Override
    public Optional<Server> fetchServer(@NotNull String id) {
        AtomicReference<Optional<Server>> server = new AtomicReference<>();
        this.loadBalancer().ifPresent(l -> server.set(l.fetchServer(id)));
        return server.get();
    }

    @Override
    public boolean containsServer(@NotNull String id) {
        AtomicBoolean value = new AtomicBoolean(false);
        this.loadBalancer().ifPresent(l -> value.set(l.containsServer(id)));
        return value.get();
    }

    @Override
    public void lockServer(@NotNull Server server) {
        this.loadBalancer().ifPresent(l -> l.lockServer(server));
    }

    @Override
    public void unlockServer(@NotNull Server server) {
        this.loadBalancer().ifPresent(l -> l.unlockServer(server));
    }

    @Override
    public List<Server> lockedServers() {
        AtomicReference<List<Server>> value = new AtomicReference<>(new ArrayList<>());
        this.loadBalancer().ifPresent(l -> value.set(l.lockedServers()));
        return value.get();
    }

    @Override
    public List<Server> unlockedServers() {
        AtomicReference<List<Server>> value = new AtomicReference<>(new ArrayList<>());
        this.loadBalancer().ifPresent(l -> value.set(l.unlockedServers()));
        return value.get();
    }

    public long players() {
        AtomicLong value = new AtomicLong(0);
        this.loadBalancer().ifPresent(l -> {
                    l.lockedServers().forEach(s -> value.addAndGet(s.players()));
                    l.unlockedServers().forEach(s -> value.addAndGet(s.players()));
                }
        );

        return value.get();
    }

    @Override
    public List<Server> servers() {
        AtomicReference<List<Server>> servers = new AtomicReference<>(new ArrayList<>());

        this.loadBalancer().ifPresent(l -> servers.set(l.servers()));

        return servers.get();
    }

    @Override
    public Optional<Server> availableServer() {
        AtomicReference<Server> server = new AtomicReference<>(null);

        this.loadBalancer().ifPresent(l -> server.set(l.availableServer().orElse(null)));

        return Optional.ofNullable(server.get());
    }

    @Override
    public boolean isLocked(@NotNull Server server) {
        AtomicBoolean valid = new AtomicBoolean(false);
        this.loadBalancer().ifPresent(l -> valid.set(l.isLocked(server)));
        return valid.get();
    }

    @Override
    public Player.Connection.Request connect(Player player, Player.Connection.Power power) {
        if(this.unlockedServers().isEmpty()) return Player.Connection.Request.failedRequest(player, "Unable to connect you to your server. Please try again later.");

        try {
            FamilyPreJoinEvent event = new FamilyPreJoinEvent(RC.P.Families().find(this.id).orElseThrow(), player, power);
            boolean canceled = RC.P.EventManager().fireEvent(event).get(1, TimeUnit.MINUTES);
            if(canceled) return Player.Connection.Request.failedRequest(player, event.canceledMessage());
        } catch (Exception ignore) {}

        try {
            HazeDatabase db = this.database.get(15, TimeUnit.SECONDS);
            Set<Residence> response;
            {
                ReadRequest query = db.newReadRequest(RESIDENCE_TABLE);
                query.withFilter(
                    Filter
                         .by("player_uuid", player.id(), Filter.EQUALS)
                        .AND("family_id", this.id(), Filter.EQUALS)
                );
                
                response = new HashSet<>(query.execute(Residence.class));
            }

            if(response.isEmpty() && this.storageProtocol == StorageProtocol.ON_FIRST_JOIN) {
                Server server = this.availableServer().orElseThrow();
                {
                    CreateRequest query = db.newCreateRequest(RESIDENCE_TABLE);
                    query.parameter("player_uuid", player.id());
                    query.parameter("server_id", server.id());
                    query.parameter("family_id", this.id());
                    query.parameter("last_joined", Instant.now());
                    
                    query.execute();
                }

                return server.connect(player, power);
            }

            Residence residence = response.stream().findAny().orElseThrow();
            LoadBalancer loadBalancer = this.loadBalancer().get(3, TimeUnit.SECONDS);
            if(this.containsServer(residence.server_id())) {
                Server server = loadBalancer.fetchServer(residence.server_id()).orElseThrow();
                
                return server.connect(player, power);
            }
            
            if(this.unavailableProtocol == UnavailableProtocol.CANCEL_CONNECTION_ATTEMPT)
                return Player.Connection.Request.failedRequest(player, "Unable to connect you to your server. Please try again later.");
            
            int remainingAttempts = loadBalancer.attempts();
            if(loadBalancer.unlockedServers().isEmpty())
                return Player.Connection.Request.failedRequest(player, Component.text("There are no available servers to connect you to! Try again later."));
            
            Player.Connection.Result connection = null;
            for (int i = 1; i <= remainingAttempts; i++) {
                Server current = loadBalancer.current().orElse(null);
                
                if(current == null)
                    return Player.Connection.Request.failedRequest(player, Component.text("There are no available servers to connect you to! Try again later."));
                
                Player.Connection.Request attempt = current.connect(player);
                try {
                    connection = attempt.result().get(10, TimeUnit.SECONDS);
                    if (connection.connected()) break;
                } catch (Exception ignore) {}
                
                loadBalancer.forceIterate();
            }
            if(connection == null)
                return Player.Connection.Request.failedRequest(player, Component.text("There are no available servers to connect you to! Try again later."));
            if(!connection.connected())
                return Player.Connection.Request.failedRequest(player, Component.text("There are no available servers to connect you to! Try again later."));
            if(connection.server() == null)
                return Player.Connection.Request.failedRequest(player, Component.text("There are no available servers to connect you to! Try again later."));
            
            if(this.unavailableProtocol == UnavailableProtocol.ASSIGN_NEW_RESIDENCE) {
//                if(this.storageProtocol != StorageProtocol.ON_FIRST_JOIN) break;

                {
                    UpdateRequest query = db.newUpdateRequest(RESIDENCE_TABLE);
                    
                    query.withFilter(
                        Filter
                             .by("player_uuid", player.id(), Filter.EQUALS)
                            .AND("family_id", this.id(), Filter.EQUALS)
                    );

                    query.parameter("player_uuid", player.id());
                    query.parameter("server_id", connection.server().id());
                    query.parameter("family_id", this.id());
                    query.parameter("last_joined", Instant.now());
                    
                    query.execute();
                }
            }
            if(this.unavailableProtocol == UnavailableProtocol.CONNECT_WITH_ERROR) {
                player.message(Component.text("The server you were supposed to connect to is unavailable. So we connected you to another server instead.", NamedTextColor.GRAY));
            }
        } catch (Exception e) {
            RC.Error(
                    Error.from(e)
                            .whileAttempting("To connect a player to their resident server")
                            .detail("Player", player.username() +" - "+player.id())
                            .detail("Family", this.id())
            );
        }
        
        return Player.Connection.Request.failedRequest(player, "Unable to connect you to your server. Please try again later.");
    }

    @Override
    public Player.Connection.Request connect(Player player) {
        return this.connect(player, Player.Connection.Power.MINIMAL);
    }

    @Override
    public @Nullable Component details() {
        AtomicReference<String> parentName = new AtomicReference<>("none");
        try {
            Flux<? extends Family> parent = this.parent().orElse(null);
            if(parent == null) throw new RuntimeException();
            parent.compute(f -> parentName.set(f.id()), ()->parentName.set("[Unavailable]"), 10, TimeUnit.SECONDS);
        } catch (Exception ignore) {}

        return join(
            newlines(),
            RC.Lang("rustyconnector-keyValue").generate("Display Name", this.displayName() == null ? "No Display Name" : this.displayName()),
            RC.Lang("rustyconnector-keyValue").generate("Parent Family", parentName.get()),
            RC.Lang("rustyconnector-keyValue").generate("Servers", this.servers().size()),
            RC.Lang("rustyconnector-keyValue").generate("Players", this.players()),
            RC.Lang("rustyconnector-keyValue").generate("Residence Expiration", this.residenceExpiration.toString()),
            RC.Lang("rustyconnector-keyValue").generate("Storage Protocol", this.storageProtocol),
            RC.Lang("rustyconnector-keyValue").generate("Unavailable Protocol", this.unavailableProtocol),
            RC.Lang("rustyconnector-keyValue").generate("Plugins", text(String.join(", ",this.modules().keySet()), BLUE)),
            space(),
            text("Extra Properties:", DARK_GRAY),
            (
                this.metadata().isEmpty() ?
                    text("There are no properties to show.", DARK_GRAY)
                    :
                    join(
                        newlines(),
                        this.metadata().entrySet().stream().map(e -> RC.Lang("rustyconnector-keyValue").generate(e.getKey(), e.getValue())).toList()
                    )
            ),
            space(),
            text("Servers:", DARK_GRAY),
            (
                this.servers().isEmpty() ?
                    text("There are no servers in this family.", DARK_GRAY)
                    :
                    join(
                        newlines(),
                        this.servers().stream().map(s->{
                            boolean locked = this.isLocked(s);
                            return join(
                                JoinConfiguration.separator(empty()),
                                text("[", DARK_GRAY),
                                text(s.id(), BLUE),
                                space(),
                                text(AddressUtil.addressToString(s.address()), YELLOW),
                                text("]:", DARK_GRAY),
                                space(),
                                (
                                        s.displayName() == null ? empty() :
                                                text(Objects.requireNonNull(s.displayName()), AQUA)
                                                        .append(space())
                                ),
                                text("(Players: ", DARK_GRAY),
                                text(s.players(), YELLOW),
                                text(")", DARK_GRAY),
                                space(),
                                (
                                        locked ? text("Locked", RED) : empty()
                                )
                            );
                        }).toList()
                    )
            )
        );
    }

    public enum UnavailableProtocol {
        CANCEL_CONNECTION_ATTEMPT,
        ASSIGN_NEW_RESIDENCE,
        CONNECT_WITH_ERROR,
        CONNECT_WITHOUT_ERROR
    }
    public enum StorageProtocol {
        ON_FIRST_JOIN,
        ON_FIRST_LEAVE
    }
}