package pl.umk.mat.faramir.cannon;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Scanner;
import org.pcj.PCJ;
import org.pcj.RegisterStorage;
import org.pcj.StartPoint;
import org.pcj.Storage;

@RegisterStorage
public class Main implements StartPoint {

    @Storage
    enum Vars {
        a, b, dummy
    }

    private Matrix a;
    private Matrix b;
    private boolean dummy;

    @Override
    public void main() throws Throwable {
        Locale.setDefault(Locale.ENGLISH);

        int sqrt = (int) Math.sqrt(PCJ.threadCount());
        if (sqrt * sqrt != PCJ.threadCount()) {
            System.err.println("Number ot threads should be perfect square");
            return;
        }
        File fileA = new File(PCJ.getProperty("A"));
        if (!fileA.isFile()) {
            System.err.println("File not found: " + fileA);
            return;
        }
        File fileB = new File(PCJ.getProperty("B"));
        if (!fileB.isFile()) {
            System.err.println("File not found: " + fileB);
            return;
        }

        int n;
        int sizePerThread;
        int myRow = PCJ.myId() / sqrt;
        int myCol = PCJ.myId() % sqrt;
        int k = (myRow + myCol) % sqrt;
        Instant startTime = Instant.now();
        Matrix matrixA;
        try (Scanner scannerA = new Scanner(new BufferedReader(new FileReader(fileA)))) {
            String line = scannerA.nextLine();
            n = line.split("\\s+").length;
            if (n % sqrt != 0) {
                System.err.println("Matrix size should be dividable by " + sqrt);
                return;
            }
            sizePerThread = n / sqrt;
            for (int y = 0; y < sizePerThread * myRow; ++y) {
                line = scannerA.nextLine();
            }
            matrixA = new Matrix(sizePerThread);
            for (int y = 0; y < sizePerThread; ++y) {
                String[] values = line.split("\\s+");
                for (int x = 0; x < sizePerThread; ++x) {
                    matrixA.setValue(y, x, Double.parseDouble(values[sizePerThread * k + x]));
                }

                if (scannerA.hasNextLine()) {
                    line = scannerA.nextLine();
                }
            }
        }

        Matrix matrixB;
        try (Scanner scannerB = new Scanner(new BufferedReader(new FileReader(fileB)))) {
            String line = scannerB.nextLine();
            for (int y = 0; y < sizePerThread * k; ++y) {
                line = scannerB.nextLine();
            }
            matrixB = new Matrix(sizePerThread);
            for (int y = 0; y < sizePerThread; ++y) {
                String[] values = line.split("\\s+");
                for (int x = 0; x < sizePerThread; ++x) {
                    matrixB.setValue(y, x, Double.parseDouble(values[sizePerThread * myCol + x]));
                }

                if (scannerB.hasNextLine()) {
                    line = scannerB.nextLine();
                }
            }
        }
        PCJ.barrier();
        if (PCJ.myId() == 0) {
            System.err.printf("Loading matrix %dx%d takes %.6f%n",
                    n, n, Duration.between(startTime, Instant.now()).toNanos() / 1e9);
        }
        Instant calcStartTime = Instant.now();

        int left = myRow * sqrt + ((myCol - 1 + sqrt) % sqrt);
        int top = ((myRow - 1 + sqrt) % sqrt) * sqrt + myCol;

        Matrix matrixC = new Matrix(sizePerThread);
        for (int i = 1; i <= sqrt; ++i) {
            Instant iterationStartTime = Instant.now();
            if (i < sqrt) {
                PCJ.asyncPut(matrixA, left, Vars.a);    // to left
                PCJ.asyncPut(matrixB, top, Vars.b);     // to up
            }

            matrixC.addMultiply(matrixA, matrixB);

            if (i < sqrt) {
                PCJ.waitFor(Vars.a);
                PCJ.waitFor(Vars.b);
                matrixA = a;
                matrixB = b;
            }

            System.err.printf("Thread-%d finished iteration %d of %d after %.6f%n",
                    PCJ.myId(), i, sqrt, Duration.between(iterationStartTime, Instant.now()).toNanos() / 1e9);
            PCJ.barrier();
            if (PCJ.myId() == 0) {
                System.err.printf("Iteration %d of %d takes %.6f%n",
                        i, sqrt, Duration.between(iterationStartTime, Instant.now()).toNanos() / 1e9);
            }
        }

        if (PCJ.myId() == 0) {
            System.err.printf("Multiplication takes %.6f%n",
                    Duration.between(calcStartTime, Instant.now()).toNanos() / 1e9);
        }

        if (!"".equals(PCJ.getProperty("print", ""))) {
            Instant printStartTime = Instant.now();
            switch (PCJ.getProperty("print")) {
                case "at0" -> {
                	    String filename = PCJ.getProperty("C").replace("#", String.valueOf(PCJ.myId()));
                	    PCJ.at(0, () -> {
                		    try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
                			    bw.write(matrixC.toString());
                	    	}
                	    });
                    }
                case "local" -> {
                	    String filename = PCJ.getProperty("C").replace("#", String.valueOf(PCJ.myId()));
                        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filename))) {
                            bw.write(matrixC.toString());
                        }
                    }
                case "stdout" -> {
                        int right = myRow * sqrt + ((myCol + 1) % sqrt);
                        int down = ((myRow + 1) % sqrt) * sqrt + myCol;
        
                        if (PCJ.myId() == 0) {
                            PCJ.put(false, 0, Vars.dummy);
                        }
                        for (int y = 0; y < sizePerThread; ++y) {
                            PCJ.waitFor(Vars.dummy);
                            for (int x = 0; x < sizePerThread; ++x) {
                                System.out.printf("%.1f ", matrixC.getValue(y, x));
                            }
                            if (myCol == sqrt - 1) {
                                System.out.println();
                            }
                            PCJ.put(true, right, Vars.dummy);
                        }
                        if (myCol == 0) {
                            PCJ.waitFor(Vars.dummy);
                            PCJ.put(true, down, Vars.dummy);
                        }
                    }
                default -> {
                    System.err.println("Print should be one of: <at0|local|stdout>!");
                }
            }
            PCJ.barrier();
            if (PCJ.myId() == 0) {
                System.err.printf("Print output takes %.6f%n",
                        Duration.between(printStartTime, Instant.now()).toNanos() / 1e9);
            }
        }
        if (PCJ.myId() == 0) {
            System.err.printf("Total execution time: %.6f%n",
                    Duration.between(startTime, Instant.now()).toNanos() / 1e9);
        }

    }

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.err.println("<nodes.txt> <A.txt> <B.txt> <C#.txt>");
            System.exit(1);
        }
        if (!new File(args[0]).isFile()) {
            System.out.println("File not found: " + args[0] + "!");
            System.exit(1);
        }
        if (!new File(args[1]).isFile()) {
            System.out.println("File not found: " + args[1] + "!");
            System.exit(1);
        }
        if (!new File(args[2]).isFile()) {
            System.out.println("File not found: " + args[2] + "!");
            System.exit(1);
        }
        PCJ.executionBuilder(Main.class)
                .addNodes(new File(args[0]))
                .addProperty("A", args[1])
                .addProperty("B", args[2])
                .addProperty("C", args[3])
		.addProperty("print", System.getProperty("print", "false"))
                .deploy();
    }
}
