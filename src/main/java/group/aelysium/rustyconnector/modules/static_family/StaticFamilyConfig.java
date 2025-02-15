package group.aelysium.rustyconnector.modules.static_family;

import group.aelysium.declarative_yaml.DeclarativeYAML;
import group.aelysium.declarative_yaml.annotations.*;
import group.aelysium.declarative_yaml.lib.Printer;
import group.aelysium.rustyconnector.common.magic_link.packet.Packet;
import group.aelysium.rustyconnector.proxy.family.load_balancing.LoadBalancerAlgorithmExchange;
import group.aelysium.rustyconnector.proxy.family.scalar_family.ScalarFamily;
import group.aelysium.rustyconnector.proxy.util.LiquidTimestamp;
import group.aelysium.rustyconnector.shaded.com.google.code.gson.gson.Gson;
import group.aelysium.rustyconnector.shaded.com.google.code.gson.gson.JsonObject;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

@Namespace("rustyconnector")
@Config("/static_families/{id}.yml")
@Comment({
        "############################################################",
        "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
        "#                      Static Family                       #",
        "#                                                          #",
        "#               ---------------------------                #",
        "#                                                          #",
        "# | Families defined here are optimized for stateful       #",
        "# | minecraft gamemodes.                                   #",
        "#                                                          #",
        "# | When players join a static family, their initial       #",
        "# | server will be memorized, and they'll be connected     #",
        "# | back into that server on future family connections     #",
        "# | as well.                                               #",
        "#                                                          #",
        "# | This initial server is called the player's residence.  #",
        "#                                                          #",
        "#               ---------------------------                #",
        "#                                                          #",
        "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
        "############################################################"
})
public class StaticFamilyConfig {
    @PathParameter("id")
    private String id;

    @Comment({
            "############################################################",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "#                       Display Name                       #",
            "#                                                          #",
            "#               ---------------------------                #",
            "# | Display name is the name of your family, as players    #",
            "# | will see it, in-game.                                  #",
            "# | Display name can appear as a result of multiple        #",
            "# | factors such as the friends module being enabled.      #",
            "#                                                          #",
            "# | Multiple families are allowed to have the              #",
            "# | same display name.                                     #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "############################################################"
    })
    @Node(0)
    private String displayName = "";

    @Comment({
            "############################################################",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "#                      Parent Family                       #",
            "#                                                          #",
            "#               ---------------------------                #",
            "# | The parent family is the family that players will      #",
            "# | be sent to when they run /hub, or when a fallback      #",
            "# | occurs. If the parent family is unavailable, the       #",
            "# | root family is used instead.                           #",
            "#                                                          #",
            "#   NOTE: If this value is set for the root family         #",
            "#         it will be ignored.                              #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "############################################################"
    })
    @Node(1)
    private String parentFamily = "";

    @Node(2)
    @Comment({
            "############################################################",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "#                      Load Balancing                      #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "# | Load balancing is the system through which networks    #",
            "# | manage player influxes by spreading out players        #",
            "# | across various server nodes.                           #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "############################################################"
    })
    private String loadBalancer = "default";

    @Node(3)
    @Comment({
            "############################################################",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "#                      Haze Database                       #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "# | Static Families require a database in order to keep    #",
            "# | track of player residences on the long-term.           #",
            "#                                                          #",
            "# | The Static Family module uses Haze Databases which     #",
            "# | are provided by RustyConnector itself.                 #",
            "#                                                          #",
            "# | You'll want to make sure you've installed the Haze     #",
            "# | Database Provider of your choice (MySQL for example)   #",
            "# | and then set the name below to be the name of          #",
            "# | the database that you registered!                      #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "############################################################"
    })
    private String database = "default";

    @Node(4)
    @Comment({
            "############################################################",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "#                   Unavailable Protocol                   #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "# | What should happen if a player tries joining the       #",
            "# | static family but their residence isn't available?     #",
            "#                                                          #",
            "# | This edge case specifically happens if the player      #",
            "# | already has a residence (which hasn't expired) and     #",
            "# | the ID of that server cannot be found.                 #",
            "#                                                          #",
            "#  ⚫ CANCEL_CONNECTION_ATTEMPT -                          #",
            "#         Do not connect the player to the family.         #",
            "#         This option will also return an error informing  #",
            "#         the player that there is not available server.   #",
            "#  ⚫ ASSIGN_NEW_RESIDENCE -                               #",
            "#         Connect the player to the family and assign      #",
            "#         them a new residence based on 'storage-protocol' #",
            "#  ⚫ CONNECT_WITH_ERROR -                                 #",
            "#         Connect the player to the family but don't       #",
            "#         change their residence.                          #",
            "#         This option will also inform the player that     #",
            "#         the typical server they play on is unavailable   #",
            "#         so they were connected to a different            #",
            "#         one instead.                                     #",
            "#  ⚫ CONNECT_WITHOUT_ERROR -                              #",
            "#         Connect the player to the family but don't       #",
            "#         change their residence.                          #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "############################################################"
    })
    private StaticFamily.UnavailableProtocol unavailableProtocol = StaticFamily.UnavailableProtocol.CONNECT_WITH_ERROR;

    @Node(5)
    @Comment({
            "############################################################",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "#                     Storage Protocol                     #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "# | The storage protocol dictates when the Static Family   #",
            "# | chooses to store the player's residence.               #",
            "#                                                          #",
            "#  ⚫ ON_FIRST_JOIN -                                      #",
            "#         Whatever server the player joins the first time  #",
            "#         that server will become the player's residence.  #",
            "#  ⚫ ON_FIRST_LEAVE -                                     #",
            "#         Whatever server the player leaves from when they #",
            "#         leave the family.                                #",
            "#         That server will become the player's residence.  #",
            "#                                                          #",
            "#   NOTE: The player's residence is only ever changed if   #",
            "#         if they didn't already have one to begin with.   #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "############################################################"
    })
    private StaticFamily.StorageProtocol storageProtocol = StaticFamily.StorageProtocol.ON_FIRST_JOIN;

    @Comment({
            "############################################################",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "#                     Storage Protocol                     #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "# | The amount of time that a player's residence is good.  #",
            "# | Once a residence expires, the player will have a new   #",
            "# | residence assigned to them next time they join.        #",
            "#                                                          #",
            "# | Any time a player joins the family, if they already    #",
            "# | have a residence, their expiration will refresh.       #",
            "# | As long as players play frequently enough, their       #",
            "# | residence will never actually expire.                  #",
            "#                                                          #",
            "#               ---------------------------                #",
            "#                                                          #",
            "#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||#",
            "############################################################"
    })
    @Node(6)
    private String residenceExpiration = "30 DAYS";

    @Node(7)
    @Comment({
            "#",
            "# Provide additional metadata for the family.",
            "# Metadata provided here is non-essential, meaning that RustyConnector is capable of running without anything provided here.",
            "# Ensure that the provided metadata conforms to valid JSON syntax.",
            "#"
    })
    private String metadata = "{\\\"serverSoftCap\\\": 30, \\\"serverHardCap\\\": 40}";

    public StaticFamily.Tinder tinder() throws IOException, ParseException {
        StaticFamily.Tinder tinder = new StaticFamily.Tinder(
                id,
                displayName.isEmpty() ? null : displayName,
                parentFamily.isEmpty() ? null : parentFamily,
                LoadBalancerConfig.New(loadBalancer).tinder(),
                this.database
        );
        tinder.storageProtocol(this.storageProtocol);
        tinder.unavailableProtocol(this.unavailableProtocol);
        tinder.residenceExpiration(LiquidTimestamp.from(this.residenceExpiration));

        Gson gson = new Gson();
        JsonObject metadataJson = gson.fromJson(this.metadata, JsonObject.class);
        metadataJson.entrySet().forEach(e->tinder.metadata(e.getKey(), Packet.Parameter.fromJSON(e.getValue()).getOriginalValue()));

        return tinder;
    }

    public static StaticFamilyConfig New(String familyID) throws IOException {
        Printer printer = new Printer()
                .pathReplacements(Map.of("id", familyID))
                .commentReplacements(Map.of("id", familyID));
        return DeclarativeYAML.From(StaticFamilyConfig.class, printer);
    }
}