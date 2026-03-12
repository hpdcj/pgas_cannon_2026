package cannon.cluster.other;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;


public class MatrixWriter {
    public static void saveMatrixPart(Matrix m, int placeId, String directoryPath) throws FileNotFoundException, IllegalArgumentException {
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            throw new IllegalArgumentException(directoryPath + " not found!");
        }

        String fileName = directoryPath + File.separator + "C.part." + placeId + ".txt";

        try (PrintWriter writer = new PrintWriter(fileName)) {
            for (int i = 0; i < m.getRows(); i++) {
                for (int j = 0; j < m.getCols(); j++) {
                    writer.print(m.get(i, j));
                    if (j < m.getCols() - 1) {
                        writer.print(" ");
                    }
                }
                writer.println();
            }
        }
    }
}
