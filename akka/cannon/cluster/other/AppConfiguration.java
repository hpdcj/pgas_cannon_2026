package cannon.cluster.other;

import cannon.cluster.CborSerializable;


public final class AppConfiguration implements CborSerializable {
    private final int workers;
    private final int matrixSize;
    private final String matrixLoadDirectoryPath;
    private final String matrixSaveDirectoryPath;

    public AppConfiguration(int workers, int matrixSize, String matrixLoadDirectoryPath, String matrixSaveDirectoryPath) {
        this.workers = workers;
        this.matrixSize = matrixSize;
        this.matrixLoadDirectoryPath = matrixLoadDirectoryPath;
        this.matrixSaveDirectoryPath = matrixSaveDirectoryPath;
    }

    public int getWorkers() {
        return workers;
    }

    public int getMatrixSize() {
        return matrixSize;
    }

    public String getMatrixLoadDirectoryPath() {
        return matrixLoadDirectoryPath;
    }

    public String getMatrixSaveDirectoryPath() {
        return matrixSaveDirectoryPath;
    }
}
