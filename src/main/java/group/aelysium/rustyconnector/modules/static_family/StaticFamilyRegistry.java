package group.aelysium.rustyconnector.modules.static_family;

import group.aelysium.rustyconnector.RC;
import group.aelysium.rustyconnector.common.modules.ExternalModuleTinder;
import group.aelysium.rustyconnector.common.modules.ModuleParticle;
import group.aelysium.rustyconnector.proxy.family.FamilyRegistry;
import group.aelysium.rustyconnector.server.ServerKernel;
import group.aelysium.rustyconnector.shaded.group.aelysium.ara.Particle;
import net.kyori.adventure.text.Component;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StaticFamilyRegistry implements ModuleParticle {
    protected Set<String> familyNames = Collections.newSetFromMap(new ConcurrentHashMap<>());

    protected StaticFamilyRegistry() {
        try {
            StaticFamilyConfig.New("default");
            File directory = new File("rc-modules/rcm-mysql");
            if(!directory.exists()) directory.mkdirs();
            for (File file : Optional.ofNullable(directory.listFiles()).orElse(new File[0])) {
                if (!(file.getName().endsWith(".yml") || file.getName().endsWith(".yaml"))) continue;
                int extensionIndex = file.getName().lastIndexOf(".");
                String name = file.getName().substring(0, extensionIndex);
                StaticFamily.Tinder tinder = StaticFamilyConfig.New(name).tinder();
                RC.P.Families().register(name, tinder);
                familyNames.add(name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public @Nullable Component details() {
        return null;
    }

    @Override
    public void close() throws Exception {
        try {
            FamilyRegistry registry = RC.P.Families();
            this.familyNames.forEach(registry::unregister);
        } catch (Exception ignore) {}
    }

    public static class Tinder extends ExternalModuleTinder<StaticFamilyRegistry> {
        public void bind(@NotNull ServerKernel kernel, @NotNull Particle instance) {
        }

        @NotNull
        @Override
        public StaticFamilyRegistry onStart() throws Exception {
            return new StaticFamilyRegistry();
        }
    }
}