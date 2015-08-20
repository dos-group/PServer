package de.tuberlin.pserver.core.config;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import de.tuberlin.pserver.core.infra.InetHelper;
import de.tuberlin.pserver.core.infra.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


public final class IConfigFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IConfigFactory.class);

    public static IConfig load(final IConfig.Type type) {
        return load(type, System.getProperty("pserver.path.config"));
    }

    /**
     * Factory method.<br>
     * <br>
     * This will construct an resolve a TypesafeConfig delegate through a merge of the following
     * configuration layers (higher number in the list means higher priority).
     *
     * 
     * <ol>
     * <li/>A classpath entry named <code>reference.${type}.conf</code> containing default values.
     * <li/>A config file named <code>reference.${type}.conf</code> containing default values
     * <li/>A config containing values from the current runtime (e.g. the number of CPU cores).
     * <li/>A classpath entry named <code>profile.${pserver.profile}.conf</code> (optional).
     * <li/>A config file named <code>profile.${pserver.profile}.conf</code> (optional).
     * <li/>A config constructed from the current System properties.
     * </ol>
     * 
     * @param type The configuration type to load.
     * @param rootDir The root directory the config files are located at. Ignored if null.
     * @return A resolved config instance constructed according to the above guidelines.
     */
    public static IConfig load(final IConfig.Type type, String rootDir) {
        // options for configuration parsing
        ConfigParseOptions opts = ConfigParseOptions.defaults().setClassLoader(TypesafeConfig.class.getClassLoader());
        // type suffix for the files at step 1 and 4
        String s = type != null ? String.format("%s.conf", type.name) : "conf";
        // 0. initial configuration is empty
        Config config = ConfigFactory.empty();
        // 1. merge a config from a classpath entry named 'reference.simulation.conf'
        config = ConfigFactory.parseResources(String.format("reference.%s", s), opts).withFallback(config);
        //LOG.error(ZookeeperClient.buildServersString(new TypesafeConfig(config.resolve()).getObjectList("zookeeper.servers")));
        // 2. if rootDir != null, merge a config file in rootDir named 'reference.simulation.conf'
        if(rootDir != null) {
            config = ConfigFactory.parseFile(new File(String.format("%s/reference.%s", rootDir, s)), opts).withFallback(config);
        }
        // 3. merge a config constructed from current system data
        config = currentRuntimeConfig().withFallback(config);
        // 4. merge a config from a classpath entry named '$profile.{pserver.profile}.conf' or fall back to profile.default.conf
        if (System.getProperty("pserver.profile") == null) {
            System.setProperty("pserver.profile", "default");
        }
        config = ConfigFactory.parseResources(String.format("profile.%s.conf", System.getProperty("pserver.profile")), opts).withFallback(config);
        // 5. if rootDir != null, merge a config file in rootDir named '$profile.{pserver.profile}.conf' or fall back to profile.default.conf
        if(rootDir != null) {
            config = ConfigFactory.parseFile(new File(String.format("%s/profile.%s.conf", rootDir, System.getProperty("pserver.profile"))), opts).withFallback(config);
        }
        // 6. merge system properties as configuration
        config = ConfigFactory.systemProperties().withFallback(config);
        // wrap the resolved delegate into a TypesafeConfig new instance and return
        return new TypesafeConfig(config.resolve());
    }

    /**
     * Loads default values from the current runtime config.
     */
    private static Config currentRuntimeConfig() {
        // initial empty configuration
        Map<String, Object> runtimeConfig = new HashMap<>();
        runtimeConfig.put("default.machine.cpu.cores", Runtime.getRuntime().availableProcessors());
        runtimeConfig.put("default.machine.memory.max", Runtime.getRuntime().maxMemory());
        runtimeConfig.put("default.machine.disk.length", new File("/").getTotalSpace()); // FIXME
        runtimeConfig.put("default.io.tcp.port", InetHelper.getFreePort());
        runtimeConfig.put("default.io.rpc.port", InetHelper.getFreePort());
        return ConfigFactory.parseMap(runtimeConfig);
    }

}
