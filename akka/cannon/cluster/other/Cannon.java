package cannon.cluster.other;

public final class Cannon {
    public static int getLeftShiftReceiverIndexInFirstStep(int index, int nodesInRow) {
        int rowIndex = index / nodesInRow;
        return (index - rowIndex + nodesInRow) % nodesInRow + nodesInRow * rowIndex;
    }

    public static int getUpperShiftReceiverIndexInFirstStep(int index, int nodesInRow) {
        int columnIndex = index % nodesInRow;
        int rowIndex = index / nodesInRow;

        return ((rowIndex - columnIndex + nodesInRow) % nodesInRow) * nodesInRow + columnIndex;
    }

    public static int getLeftShiftByOneReceiverIndex(int index, int nodesInRow) {
        int rowIndex = index / nodesInRow;
        return (index - 1 + nodesInRow) % nodesInRow + nodesInRow * rowIndex;
    }

    public static int getUpperShiftByOneReceiverIndex(int index, int nodesInRow) {
        int columnIndex = index % nodesInRow;
        int rowIndex = index / nodesInRow;

        return ((rowIndex - 1 + nodesInRow) % nodesInRow) * nodesInRow + columnIndex;
    }

    public static int getLeftShiftByOneSenderIndex(int index, int nodesInRow) {
        int rowIndex = index / nodesInRow;
        return (index + 1) % nodesInRow + nodesInRow * rowIndex;
    }

    public static int getUpperShiftByOneSenderIndex(int index, int nodesInRow) {
        int columnIndex = index % nodesInRow;
        int rowIndex = index / nodesInRow;

        return ((rowIndex + 1) % nodesInRow) * nodesInRow + columnIndex;
    }

    public static int getLeftShiftSenderIndexInFirstStep(int index, int nodesInRow) {
        int rowIndex = index / nodesInRow;
        return (index + rowIndex) % nodesInRow + nodesInRow * rowIndex;
    }

    public static int getUpperShiftSenderIndexInFirstStep(int index, int nodesInRow) {
        int columnIndex = index % nodesInRow;
        int rowIndex = index / nodesInRow;

        return ((rowIndex + columnIndex) % nodesInRow) * nodesInRow + columnIndex;
    }
}
