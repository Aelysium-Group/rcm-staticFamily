package group.aelysium.rustyconnector.modules.static_family;

import group.aelysium.rustyconnector.RC;
import group.aelysium.rustyconnector.common.errors.Error;
import group.aelysium.rustyconnector.common.magic_link.packet.Packet;
import group.aelysium.rustyconnector.common.modules.ExternalModuleBuilder;
import group.aelysium.rustyconnector.common.modules.Module;
import group.aelysium.rustyconnector.proxy.ProxyKernel;
import group.aelysium.rustyconnector.proxy.family.Family;
import group.aelysium.rustyconnector.proxy.family.FamilyRegistry;
import group.aelysium.rustyconnector.proxy.family.load_balancing.LoadBalancerRegistry;
import group.aelysium.rustyconnector.proxy.util.LiquidTimestamp;
import group.aelysium.rustyconnector.shaded.com.google.code.gson.gson.Gson;
import group.aelysium.rustyconnector.shaded.com.google.code.gson.gson.JsonObject;
import group.aelysium.rustyconnector.shaded.group.aelysium.declarative_yaml.DeclarativeYAML;
import net.kyori.adventure.text.Component;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class StaticFamilyProvider implements Module {
    @Override
    public @Nullable Component details() {
        return null;
    }

    @Override
    public void close() throws Exception {}

    public static class Builder extends ExternalModuleBuilder<StaticFamilyProvider> {
        public void bind(@NotNull ProxyKernel kernel, @NotNull StaticFamilyProvider instance) {
            kernel.fetchModule("FamilyRegistry").onStart(f -> {
                FamilyRegistry registry = (FamilyRegistry) f;
                
                try {
                    File directory = new File(DeclarativeYAML.basePath("rustyconnector")+"/static_families");
                    if(!directory.exists()) directory.mkdirs();
                    
                    {
                        File[] files = directory.listFiles();
                        if (files == null || files.length == 0)
                            StaticFamilyConfig.New("my-first-static-family");
                    }
                    
                    File[] files = directory.listFiles();
                    if (files == null) return;
                    if (files.length == 0) return;
                    
                    for (File file : files) {
                        if (!(file.getName().endsWith(".yml") || file.getName().endsWith(".yaml"))) continue;
                        int extensionIndex = file.getName().lastIndexOf(".");
                        String name = file.getName().substring(0, extensionIndex);
                        RC.P.Families().register(name, new Module.Builder<>("StaticFamily", "Provides predictable player connections to server based on database-stored context.") {
                            @Override
                            public Family get() {
                                try {
                                    StaticFamilyConfig config = StaticFamilyConfig.New(name);
                                    
                                    LoadBalancerRegistry r = RC.Module("LoadBalancerRegistry");
                                    
                                    Gson gson = new Gson();
                                    JsonObject metadataJson = gson.fromJson(config.metadata, JsonObject.class);
                                    Map<String, Object> mt = new HashMap<>();
                                    metadataJson.entrySet().forEach(e->mt.put(e.getKey(), Packet.Parameter.fromJSON(e.getValue()).getOriginalValue()));
                                    
                                    return new StaticFamily(
                                        name,
                                        config.displayName,
                                        config.parentFamily,
                                        mt,
                                        r.generate(config.loadBalancer),
                                        LiquidTimestamp.from(config.residenceExpiration),
                                        config.unavailableProtocol,
                                        config.storageProtocol,
                                        config.database
                                    );
                                } catch (Exception e) {
                                    RC.Error(Error.from(e).whileAttempting("To generate the static family "+name));
                                }
                                return null;
                            }
                        });
                    }
                } catch (Exception e) {
                    RC.Error(Error.from(e).whileAttempting("To bind StaticFamilyProvider to the FamilyRegistry."));
                }
            });
        }
        
        @NotNull
        @Override
        public StaticFamilyProvider onStart(@NotNull Path dataDirectory) throws Exception {
            return new StaticFamilyProvider();
        }
    }
}