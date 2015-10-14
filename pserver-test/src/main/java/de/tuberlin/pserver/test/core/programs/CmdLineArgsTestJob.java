package de.tuberlin.pserver.test.core.programs;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

import static org.junit.Assert.*;


public class CmdLineArgsTestJob extends Program {

    final String[] testArgs = {"this", "is", "a", "test"};

    @Unit
    public void unit(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            int errors = 0;

            if (programContext.args.length > 0) {
                for (int i = 0; i < programContext.args.length; i++) {
                    if ( ! testArgs[i].equals(programContext.args[i])) {
                        errors++;
                    }
                }
            }

            if (errors > 0) {
                result(false);
            } else {
                result(true);
            }
        });
    }

    @Test
    public void testCommandLineArgs() throws InterruptedException {
        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.LOCAL
                .arguments(testArgs)
                .run(CmdLineArgsTestJob.class)
                .results(result)
                .done();

        assertTrue((boolean) result.get(0).get(0));
    }

    // these two test cases can't be executed consecutively because it throws
    // java.net.BindException: Address already in use

    /*
    @Test
    public void testWithoutCommandLineArgs() throws InterruptedException {
        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(CmdLineArgsTestJob.class)
                .results(result)
                .done();

        assertTrue((boolean) result.get(0).get(0));
    }*/
}
