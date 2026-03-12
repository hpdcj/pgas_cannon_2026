import java.io.Serializable;

import static apgas.Constructs.async;
import static apgas.Constructs.finish;

public final class IterationMatrix implements Serializable {
    private static final long serialVersionUID = 5213701315312394081L;

    private final Matrix matrix;
    private final int iteration;

    public IterationMatrix(Matrix matrix, int iteration) {
        this.matrix = matrix;
        this.iteration = iteration;
    }

    public Matrix getMatrix() {
        return matrix;
    }

    public boolean isFromCurrentIteration(int currentIteration) {
        return iteration == currentIteration;
    }
}
