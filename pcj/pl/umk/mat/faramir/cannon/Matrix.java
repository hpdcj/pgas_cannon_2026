package pl.umk.mat.faramir.cannon;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Matrix implements Serializable {
    private final double[][] matrix;

    public Matrix(int n) {
        matrix = new double[n][n];
    }

    public void setValue(int y, int x, double value) {
        matrix[y][x] = value;
    }

    public double getValue(int y, int x) {
        return matrix[y][x];
    }

    public void addMultiply(Matrix a, Matrix b) {
        try (ExecutorService virtualThreads = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int y = 0; y < matrix.length; ++y) {
                int _y = y;
                virtualThreads.submit(() -> {
                    for (int k = 0; k < matrix.length; ++k) {
                        double aYK = a.matrix[_y][k];
                        for (int x = 0; x < matrix[_y].length; ++x) {
                            matrix[_y][x] += aYK * b.matrix[k][x];
                        }
                    }
                });
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (double[] row : matrix) {
            for (double m : row) {
                sb.append(String.format("%g", m));
                sb.append('\t');
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
