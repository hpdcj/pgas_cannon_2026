import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class Matrix implements Serializable {
    private static final long serialVersionUID = 1625040027880477023L;

    private final double[][] matrix;
    private final int rows;
    private final int cols;

    public Matrix(double[][] inputMatrix) {
        this.rows = inputMatrix.length;
        this.cols = inputMatrix[0].length;
        this.matrix = new double[rows][cols];

        for (int i = 0; i < rows; i++) {
            if (inputMatrix[i].length != this.cols) {
                throw new IllegalArgumentException("All rows must have the same number of columns!");
            }

            this.matrix[i] = inputMatrix[i].clone();
        }
    }

    public Matrix(int rows, int cols, double fillWith) {
        this.rows = rows;
        this.cols = cols;
        this.matrix = new double[rows][cols];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                this.matrix[i][j] = fillWith;
            }
        }
    }

    public int getRows() {
        return rows;
    }

    public int getCols() {
        return cols;
    }

    public double get(int row, int col) {
        if (row < 0 || row >= rows || col < 0 || col >= cols) {
            throw new IndexOutOfBoundsException("Invalid index");
        }
        return matrix[row][col];
    }

    public static ExecutorService EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public void addMultiply(Matrix a, Matrix b) {
        List<Future<?>> futures = new ArrayList<>();
        for (int y = 0; y < matrix.length; ++y) {
            int _y = y;
            futures.add(EXECUTOR.submit(() -> {
                for (int k = 0; k < matrix.length; ++k) {
                    double aYK = a.matrix[_y][k];
                    for (int x = 0; x < matrix[_y].length; ++x) {
                        matrix[_y][x] += aYK * b.matrix[k][x];
                    }
                }
            }, EXECUTOR));
        }

        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
//    public void addMultiply(Matrix a, Matrix b) {
//        try (ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor()) {
//        {
//            for (int y = 0; y < matrix.length; ++y) {
//                int _y = y;
//                virtualThreads.submit(() -> {
//                    for (int k = 0; k < matrix.length; ++k) {
//                        double aYK = a.matrix[_y][k];
//                        for (int x = 0; x < matrix[_y].length; ++x) {
//                            matrix[_y][x] += aYK * b.matrix[k][x];
//                        }
//                    }
//                });
//            }
//        }
//    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Matrix)) return false;

        Matrix other = (Matrix) obj;

        if (this.rows != other.rows || this.cols != other.cols) {
            return false;
        }

        for (int i = 0; i < this.rows; i++) {
            for (int j = 0; j < this.cols; j++) {
                if (this.get(i, j) != other.get(i, j)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                sb.append(matrix[i][j]);
                if (j < cols - 1) {
                    sb.append(", ");
                }
            }
            if (i < rows - 1) {
                sb.append(" | ");
            }
        }

        sb.append("]");
        return sb.toString();
    }
}
