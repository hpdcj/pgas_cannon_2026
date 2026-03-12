import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;


public class MatrixReader {
    public static Matrix readSquareMatrixFromFile(String fileName) throws FileNotFoundException, IllegalArgumentException {
        Scanner scanner = new Scanner(new File(fileName));
        List<double[]> matrixRows = new ArrayList<>();

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();
            String[] values = line.split("\\s+");

            double[] row = new double[values.length];

            for (int i = 0; i < values.length; i++) {
                row[i] = Double.parseDouble(values[i]);
            }

            matrixRows.add(row);
        }

        scanner.close();

        if (!isMatrixSquare(matrixRows)) {
            throw new IllegalArgumentException("Matrix is not square!");
        }

        int matrixSize = matrixRows.size();
        double[][] matrix = new double[matrixSize][matrixSize];

        for (int i = 0; i < matrixSize; i++) {
            matrix[i] = matrixRows.get(i);
        }

        return new Matrix(matrix);
    }

    public static Matrix readAndSplitSquareMatrixFromFile(String fileName, int nodeIndex, int nodesInRow) throws FileNotFoundException, IllegalArgumentException {
        Scanner scanner = new Scanner(new File(fileName));
        List<double[]> matrixRows = new ArrayList<>();
        int rowIndex = 0;

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine().trim();
            String[] values = line.split("\\s+");
            int matrixSize = values.length;
            int partSize = matrixSize / nodesInRow; // 2
            int nodeStartRowIndex = (nodeIndex / nodesInRow) * partSize; // 0
            int nodeStartColumnIndex = (nodeIndex % nodesInRow);

            if (rowIndex >= nodeStartRowIndex && rowIndex < (nodeStartRowIndex + partSize)) {
                double[] row = new double[partSize];
                int j = 0;
                for (int i = nodeStartColumnIndex * partSize; i < (nodeStartColumnIndex + 1) * partSize; i++) {
                    row[j] = Double.parseDouble(values[i]);
                    j++;
                }

                matrixRows.add(row);
            }
            rowIndex++;
        }
        scanner.close();

        int partMatrixSize = matrixRows.size();
        double[][] matrix = new double[partMatrixSize][partMatrixSize];

        for (int i = 0; i < partMatrixSize; i++) {
            matrix[i] = matrixRows.get(i);
        }

        return new Matrix(matrix);
    }

    private static boolean isMatrixSquare(List<double[]> matrixRows) {
        if (matrixRows.isEmpty()) {
            return false;
        }

        for (int i = 0; i < matrixRows.size(); i++) {
            if (matrixRows.size() != matrixRows.get(i).length) {
                return false;
            }
        }

        return true;
    }
}
