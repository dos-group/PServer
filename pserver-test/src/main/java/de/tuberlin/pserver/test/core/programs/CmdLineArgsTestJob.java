package de.tuberlin.pserver.test.core.programs;

import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;


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
}
