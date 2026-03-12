import apgas.Configuration;
import apgas.Place;
import apgas.util.GlobalRef;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static apgas.Constructs.*;

final class MainWithoutSemaphores {
    private static int aRows;
    private static int bCols;

    public static void main(String[] args) {
        if (args.length < 4 || args.length > 5) {
            System.out.println("Required params: <places> <matrix A path> <matrix B path> <results directory path> [<hosts file path> = null]");
            System.exit(1);
        }
        if (args.length == 5) {
            if (System.getProperty(Configuration.APGAS_HOSTFILE) == null) {
                System.out.println("Setting hosts file to " + args[4]);
                System.setProperty(Configuration.APGAS_HOSTFILE, args[4]);
            }
        }
        final int workers = Integer.parseInt(args[0]);
        int sqrt = (int) Math.sqrt(workers);
        if (workers < 1 || (sqrt * sqrt) != workers) {
            System.out.println("Number of places has to be the square of any unsigned integer, other than 0");
            System.exit(1);
        }
        final int jvmAvailableProcessors = Runtime.getRuntime().availableProcessors();
        if (jvmAvailableProcessors < 2 && System.getProperty(Configuration.APGAS_THREADS) == null) {
            System.setProperty(Configuration.APGAS_THREADS, String.valueOf(2));
        }
        int workersInRow = (int) Math.sqrt(workers);
        final String resultsDirectory = args[3];

        // Run with given number of places
        if (System.getProperty(Configuration.APGAS_PLACES) == null) {
            System.out.println("Setting number of places to " + workers);
            System.setProperty(Configuration.APGAS_PLACES, String.valueOf(workers));
        }

        // Initialize GlobalRef with matrices from specified files
        GlobalRef<ArrayBlockingQueue<IterationMatrix>> AReceivedQueue = new GlobalRef<>(places(), () -> {
            Matrix A = MatrixReader.readAndSplitSquareMatrixFromFile(args[1], here().id, workersInRow);
            aRows = A.getRows();
            ArrayBlockingQueue<IterationMatrix> queue = new ArrayBlockingQueue<>(workersInRow);
            queue.add(new IterationMatrix(A, 0));

            return queue;
        });

        GlobalRef<ArrayBlockingQueue<IterationMatrix>> BReceivedQueue = new GlobalRef<>(places(), () -> {
            Matrix B = MatrixReader.readAndSplitSquareMatrixFromFile(args[2], here().id, workersInRow);
            bCols = B.getCols();
            ArrayBlockingQueue<IterationMatrix> queue = new ArrayBlockingQueue<>(workersInRow);
            queue.add(new IterationMatrix(B, 0));

            return queue;
        });

        System.out.println("Running main at " + here() + " of " + places().size() + " places");
        System.out.println("Loaded Matrices, starting calculation...\n");

        finish(() -> {
            for (final Place place : places()) {
                asyncAt(place, () -> {
                    Matrix C = new Matrix(aRows, bCols, 0);
                    ArrayBlockingQueue<Matrix> aToSend = new ArrayBlockingQueue<>(1);
                    ArrayBlockingQueue<Matrix> bToSend = new ArrayBlockingQueue<>(1);
                    Matrix moveA = AReceivedQueue.get().take().getMatrix();
                    Matrix moveB = BReceivedQueue.get().take().getMatrix();
                    aToSend.add(moveA);
                    bToSend.add(moveB);
                    AtomicInteger iteration = new AtomicInteger(0);
                    long startTime = System.nanoTime();

                    for (int i = 0; i < workersInRow; i++) {
                        Matrix readMatrixA = aToSend.take();
                        Matrix readMatrixB = bToSend.take();
                        int currentIteration = iteration.getAndIncrement();
                        boolean isFirstIteration = i == 0;
                        int leftShiftNodeIndex = isFirstIteration ? Cannon.getLeftShiftReceiverIndexInFirstStep(place.id, workersInRow)
                                : Cannon.getLeftShiftByOneReceiverIndex(place.id, workersInRow);
                        int upShiftNodeIndex = isFirstIteration ? Cannon.getUpperShiftReceiverIndexInFirstStep(place.id, workersInRow)
                                : Cannon.getUpperShiftByOneReceiverIndex(place.id, workersInRow);

                        // send Matrix to the place on the left
                        asyncAt(place(leftShiftNodeIndex), () -> {
                            AReceivedQueue.get().add(new IterationMatrix(readMatrixA, currentIteration));
                        });

                        // send Matrix to the place on the up
                        asyncAt(place(upShiftNodeIndex), () -> {
                            BReceivedQueue.get().add(new IterationMatrix(readMatrixB, currentIteration));
                        });

                        Matrix tmpA = getCurrentIterationMatrix(AReceivedQueue, currentIteration);
                        Matrix tmpB = getCurrentIterationMatrix(BReceivedQueue, currentIteration);

                        C.addMultiply(tmpA, tmpB);

                        aToSend.add(tmpA);
                        bToSend.add(tmpB);
                    }

                    System.out.println("[Place " + place.id + "] Time: " + (System.nanoTime() - startTime) / 1e9);
                    MatrixWriter.saveMatrixPart(C, place.id, resultsDirectory);
                });
            }
        });

        System.out.println("All places finished their jobs...");
        Matrix.EXECUTOR.shutdown();
    }

    private static Matrix getCurrentIterationMatrix(GlobalRef<ArrayBlockingQueue<IterationMatrix>> queue, int currentIteration) throws InterruptedException {
        IterationMatrix tmpIterationMatrix = queue.get().take();
        if (tmpIterationMatrix.isFromCurrentIteration(currentIteration)) {
            return tmpIterationMatrix.getMatrix();
        }

        Queue<IterationMatrix> tmpQueue = new LinkedList<>();
        tmpQueue.add(tmpIterationMatrix);
        while (true) {
            IterationMatrix currentIterationMatrix = queue.get().take();
            if (currentIterationMatrix.isFromCurrentIteration(currentIteration)) {
                final int size = tmpQueue.size();
                for (int i = 0; i < size; ++i) {
                    IterationMatrix tmp = tmpQueue.remove();
                    queue.get().add(tmp);
                }

                return currentIterationMatrix.getMatrix();
            }

            tmpQueue.add(currentIterationMatrix);
        }
    }
}
