package de.tuberlin.pserver.runtime.core.config;


import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;

public final class ConfigLoader {

    /*public static Config loadFile(String filePath) {
        ConfigParseOptions opts = ConfigParseOptions.defaults()
                .setClassLoader(TypesafeConfig.class.getClassLoader());
        com.typesafe.config.Config config = ConfigFactory.parseFile(new File(filePath), opts);
        config = ConfigFactory.systemProperties().withFallback(config);
        return new TypesafeConfig(config.resolve());
    }*/

    public static Config loadResource(String resource) {
        ConfigParseOptions opts = ConfigParseOptions.defaults()
                .setClassLoader(TypesafeConfig.class.getClassLoader());
        com.typesafe.config.Config config = ConfigFactory.parseResources(resource, opts);
        config = ConfigFactory.systemProperties().withFallback(config);
        return new TypesafeConfig(config.resolve());
    }
}
