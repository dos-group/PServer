package de.tuberlin.pserver.node;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.impl.type.StringArgumentType;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PServerMain {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final Logger LOG = LoggerFactory.getLogger(PServerMain.class);

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    private static ArgumentParser getArgumentParser() {
        //@formatter:off
        ArgumentParser parser = ArgumentParsers.newArgumentParser("pserver-node")
                .defaultHelp(true)
                .description("pserver");

        parser.addArgument("--config-dir")
                .type(new FileArgumentType().verifyIsDirectory().verifyCanRead())
                .dest("pserver.path.config")
                .metavar("PATH")
                .help("config folder");

        parser.addArgument("--profile")
                .type(new StringArgumentType())
                .dest("pserver.profile")
                .metavar("PROFILE")
                .help("pserver profile");

        //@formatter:on
        return parser;
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) {

        // construct base argument parser
        ArgumentParser parser = getArgumentParser();
        try {
            // parse the arguments and store them as system properties
            parser.parseArgs(args).getAttrs().entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .forEach(e -> System.setProperty(e.getKey(), e.getValue().toString()));

            PServerNodeFactory.createNode();

        } catch (HelpScreenException e) {
            parser.handleError(e);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Throwable e) {
            System.err.println(String.format("Unexpected error: %s", e));
            e.printStackTrace();
            System.exit(1);
        }
    }
}
