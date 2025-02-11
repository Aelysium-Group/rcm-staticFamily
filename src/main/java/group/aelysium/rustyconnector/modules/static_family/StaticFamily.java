package group.aelysium.rustyconnector.modules.static_family;
import group.aelysium.rustyconnector.RC;
import group.aelysium.rustyconnector.common.errors.Error;
import group.aelysium.rustyconnector.common.haze.HazeDatabase;
import group.aelysium.rustyconnector.common.modules.ModuleTinder;
import group.aelysium.rustyconnector.proxy.events.FamilyPreJoinEvent;
import group.aelysium.rustyconnector.proxy.family.Family;
import group.aelysium.rustyconnector.proxy.family.Server;
import group.aelysium.rustyconnector.proxy.family.load_balancing.LoadBalancer;
import group.aelysium.rustyconnector.proxy.player.Player;
import group.aelysium.rustyconnector.proxy.util.LiquidTimestamp;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.exceptions.HazeCastingException;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.exceptions.HazeException;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.lib.DataHolder;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.lib.DataKey;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.lib.Filterable;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.query.CreateRequest;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.query.ReadRequest;
import group.aelysium.rustyconnector.shaded.group.aelysium.haze.query.UpdateRequest;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class StaticFamily extends Family {
    private static final String RESIDENCE_TABLE = "staticFamily_residence";

    protected final LiquidTimestamp homeServerExpiration;
    protected final UnavailableProtocol unavailableProtocol;
    protected final StorageProtocol storageProtocol;
    protected final String databaseName;
    protected final Flux<? extends HazeDatabase> database;

    protected StaticFamily(
            @NotNull String id,
            @Nullable String displayName,
            @Nullable String parent,
            @NotNull Map<String, Object> metadata,
            @NotNull ModuleTinder<? extends LoadBalancer> loadBalancer,
            @NotNull LiquidTimestamp homeServerExpiration,
            @NotNull UnavailableProtocol unavailableProtocol,
            @NotNull StorageProtocol storageProtocol,
            @NotNull String database
    ) throws Exception {
        super(id, displayName, parent, metadata);
        this.registerModule(loadBalancer);
        this.unavailableProtocol = unavailableProtocol;
        this.homeServerExpiration = homeServerExpiration;
        this.storageProtocol = storageProtocol;
        this.databaseName = database;

        this.database = (Flux<? extends HazeDatabase>) RC.P.Haze().fetchDatabase(this.databaseName)
                .orElseThrow(()->new NoSuchElementException("No database exists on the haze provider with the name '"+this.databaseName+"'."));
        HazeDatabase db = this.database.observe(1, TimeUnit.MINUTES);
        if(db.doesDataHolderExist(RESIDENCE_TABLE)) return;

        DataHolder table = new DataHolder(RESIDENCE_TABLE);
        List<DataKey> columns = List.of(
                new DataKey("player_uuid", DataKey.DataType.STRING).length(36).nullable(false),
                new DataKey("server_id", DataKey.DataType.STRING).length(64).nullable(false),
                new DataKey("family_id", DataKey.DataType.STRING).length(16).nullable(false),
                new DataKey("last_joined", DataKey.DataType.DATETIME).nullable(false)
        );
        columns.forEach(table::addKey);
        db.createDataHolder(table);
    }

    public @NotNull UnavailableProtocol unavailableProtocol() {
        return this.unavailableProtocol;
    }

    public @NotNull StorageProtocol storageProtocol() {
        return this.storageProtocol;
    }

    public @NotNull LiquidTimestamp homeServerExpiration() {
        return this.homeServerExpiration;
    }

    public @NotNull Flux<? extends HazeDatabase> database() {
        return this.database;
    }

    public Flux<? extends LoadBalancer> loadBalancer() {
        return this.fetchModule("LoadBalancer");
    }

    public void addServer(@NotNull Server server) {
        this.loadBalancer().executeNow(l -> l.addServer(server));
    }

    public void removeServer(@NotNull Server server) {
        this.loadBalancer().executeNow(l -> l.removeServer(server));
    }

    @Override
    public Optional<Server> fetchServer(@NotNull String id) {
        AtomicReference<Optional<Server>> server = new AtomicReference<>();
        this.loadBalancer().executeNow(l -> server.set(l.fetchServer(id)));
        return server.get();
    }

    @Override
    public boolean containsServer(@NotNull String id) {
        AtomicBoolean value = new AtomicBoolean(false);
        this.loadBalancer().executeNow(l -> value.set(l.containsServer(id)));
        return value.get();
    }

    @Override
    public void lockServer(@NotNull Server server) {
        this.loadBalancer().executeNow(l -> l.lockServer(server));
    }

    @Override
    public void unlockServer(@NotNull Server server) {
        this.loadBalancer().executeNow(l -> l.unlockServer(server));
    }

    @Override
    public List<Server> lockedServers() {
        AtomicReference<List<Server>> value = new AtomicReference<>(new ArrayList<>());
        this.loadBalancer().executeNow(l -> value.set(l.lockedServers()));
        return value.get();
    }

    @Override
    public List<Server> unlockedServers() {
        AtomicReference<List<Server>> value = new AtomicReference<>(new ArrayList<>());
        this.loadBalancer().executeNow(l -> value.set(l.unlockedServers()));
        return value.get();
    }

    public long players() {
        AtomicLong value = new AtomicLong(0);
        this.loadBalancer().executeNow(l -> {
                    l.lockedServers().forEach(s -> value.addAndGet(s.players()));
                    l.unlockedServers().forEach(s -> value.addAndGet(s.players()));
                }
        );

        return value.get();
    }

    @Override
    public List<Server> servers() {
        AtomicReference<List<Server>> servers = new AtomicReference<>(new ArrayList<>());

        this.loadBalancer().executeNow(l -> servers.set(l.servers()));

        return servers.get();
    }

    @Override
    public Optional<Server> availableServer() {
        AtomicReference<Server> server = new AtomicReference<>(null);

        this.loadBalancer().executeNow(l -> server.set(l.availableServer().orElse(null)));

        return Optional.ofNullable(server.get());
    }

    @Override
    public boolean isLocked(@NotNull Server server) {
        AtomicBoolean valid = new AtomicBoolean(false);
        this.loadBalancer().executeNow(l -> valid.set(l.isLocked(server)));
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
            HazeDatabase db = this.database.observe(15, TimeUnit.SECONDS);
            Set<Residence> response;
            try (ReadRequest query = db.newReadRequest(RESIDENCE_TABLE)) {
                query.filters().filterBy("player_uuid", new Filterable.FilterValue(player.uuid().toString(), Filterable.Qualifier.EQUALS));
                query.filters().filterBy("family_id", new Filterable.FilterValue(this.id(), Filterable.Qualifier.EQUALS));
                response = new HashSet<>(query.execute(Residence.class));
            }

            if(response.isEmpty() && this.storageProtocol == StorageProtocol.ON_FIRST_JOIN) {
                Server server = this.availableServer().orElseThrow();
                try (CreateRequest query = db.newCreateRequest(RESIDENCE_TABLE)) {
                    query.parameter("player_uuid", player.uuid().toString());
                    query.parameter("server_id", server.id());
                    query.parameter("family_id", this.id());
                    query.parameter("last_joined", Instant.now());
                }

                return server.connect(player, power);
            }

            Residence residence = response.stream().findAny().orElseThrow();
            Server server = null;
            if(!this.containsServer(residence.server_id()))
                switch (this.unavailableProtocol) {
                    case CANCEL_CONNECTION_ATTEMPT -> {
                        return Player.Connection.Request.failedRequest(player, "Unable to connect you to your server. Please try again later.");
                    }
                    case ASSIGN_NEW_RESIDENCE -> {
                        server = this.availableServer().orElseThrow();

                        if(this.storageProtocol != StorageProtocol.ON_FIRST_JOIN) break;

                        try (UpdateRequest query = db.newUpdateRequest(RESIDENCE_TABLE)) {
                            query.filters().filterBy("player_uuid", new Filterable.FilterValue(player.uuid().toString(), Filterable.Qualifier.EQUALS));
                            query.filters().filterBy("family_id", new Filterable.FilterValue(this.id(), Filterable.Qualifier.EQUALS));

                            query.parameter("player_uuid", player.uuid().toString());
                            query.parameter("server_id", server.id());
                            query.parameter("family_id", this.id());
                            query.parameter("last_joined", Instant.now());
                        }
                    }
                    case CONNECT_WITH_ERROR -> {
                        server = this.availableServer().orElseThrow();
                        player.message(Component.text("The server you were supposed to connect to is unavailable. So we're trying to connect you to another server instead...", NamedTextColor.GRAY));
                    }
                }

            if(server == null) return Player.Connection.Request.failedRequest(player, "Unable to connect you to your server. Please try again later.");
            return server.connect(player, power);
        } catch (Exception e) {
            RC.Error(
                    Error.from(e)
                            .whileAttempting("To connect a player to their resident server")
                            .detail("Player", player.username() +" - "+player.uuid())
                            .detail("Family", this.id())
            );
            return Player.Connection.Request.failedRequest(player, "Unable to connect you to your server. Please try again later.");
        }
    }

    @Override
    public Player.Connection.Request connect(Player player) {
        return this.connect(player, Player.Connection.Power.MINIMAL);
    }

    public static class Tinder extends ModuleTinder<StaticFamily> {
        private final String id;
        private final String displayName;
        private final String parent;
        private final Map<String, Object> metadata = new HashMap<>();
        private final LoadBalancer.Tinder<?> loadBalancer;
        private final String database;
        private LiquidTimestamp residenceExpiration = LiquidTimestamp.from(30, TimeUnit.DAYS);
        private StorageProtocol storageProtocol = StorageProtocol.ON_FIRST_JOIN;
        private UnavailableProtocol unavailableProtocol = UnavailableProtocol.ASSIGN_NEW_RESIDENCE;

        public Tinder(
                @NotNull String id,
                @Nullable String displayName,
                @Nullable String parent,
                @NotNull LoadBalancer.Tinder<?> loadBalancer,
                @NotNull String database
        ) {
            super(
                    "ScalarFamily",
                    "Provides load balancing services for stateless servers."
            );
            this.id = id;
            this.displayName = displayName;
            this.parent = parent;
            this.loadBalancer = loadBalancer;
            this.database = database;
        }

        public Tinder metadata(@NotNull String key, @NotNull Object value) {
            this.metadata.put(key, value);
            return this;
        }

        public Tinder unavailableProtocol(@NotNull UnavailableProtocol unavailableProtocol) {
            this.unavailableProtocol = unavailableProtocol;
            return this;
        }

        public Tinder storageProtocol(@NotNull StorageProtocol storageProtocol) {
            this.storageProtocol = storageProtocol;
            return this;
        }

        public Tinder residenceExpiration(@NotNull LiquidTimestamp residenceExpiration) {
            this.residenceExpiration = residenceExpiration;
            return this;
        }

        @Override
        public @NotNull StaticFamily ignite() throws Exception {
            return new StaticFamily(
                    this.id,
                    this.displayName,
                    this.parent,
                    this.metadata,
                    this.loadBalancer,
                    this.residenceExpiration,
                    this.unavailableProtocol,
                    this.storageProtocol,
                    this.database
            );
        }
    }

    public enum UnavailableProtocol {
        CANCEL_CONNECTION_ATTEMPT,
        ASSIGN_NEW_RESIDENCE,
        CONNECT_WITH_ERROR
    }
    public enum StorageProtocol {
        ON_FIRST_JOIN,
        ON_FIRST_LEAVE
    }
}