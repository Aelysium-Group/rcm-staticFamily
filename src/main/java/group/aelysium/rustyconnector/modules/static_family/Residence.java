package group.aelysium.rustyconnector.modules.static_family;

import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.UUID;

public record Residence(
        int id,
        @NotNull UUID player_uuid,
        @NotNull String server_id,
        @NotNull String family_id,
        @NotNull LocalDateTime last_joined
) {
}
