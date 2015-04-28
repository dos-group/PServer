package de.tuberlin.pserver.node;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.ArgumentParser;
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
                .setDefault("config")
                .metavar("PATH")
                .help("config folder");
        //@formatter:on
        return parser;
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {
        // construct base argument parser
        /*ArgumentParser parser = getArgumentParser();
        try {
            // parse the arguments and store them as system properties
            parser.parseArgs(args).getAttrs().entrySet().stream()
                    .filter(e -> e.getValueOfColumn() != null)
                    .forEach(e -> System.setProperty(e.getKey(), e.getValueOfColumn().toString()));*/

            PServerNodeFactory.createParameterServerNode();

        /*} catch (HelpScreenException e) {
            parser.handleError(e);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Throwable e) {
            System.err.println(String.format("Unexpected error: %s", e));
            e.printStackTrace();
            System.exit(1);
        }*/
    }
}
