package cannon.cluster.other;

import cannon.cluster.CborSerializable;
import com.fasterxml.jackson.annotation.JsonCreator;


public final class IterationMatrix implements CborSerializable {
    private final Matrix matrix;
    private final int iteration;

    @JsonCreator
    public IterationMatrix(Matrix matrix, int iteration) {
        this.matrix = matrix;
        this.iteration = iteration;
    }

    public Matrix getMatrix() {
        return matrix;
    }
    public int getIteration() { return iteration; }

    public boolean isFromCurrentIteration(int currentIteration) {
        return iteration == currentIteration;
    }
}
