package cannon.cluster.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import cannon.cluster.CborSerializable;
import cannon.cluster.other.Cannon;
import cannon.cluster.other.Matrix;
import cannon.cluster.other.AppConfiguration;
import cannon.cluster.other.IterationMatrix;

import java.util.LinkedList;
import java.util.Queue;

public class CannonActor extends AbstractBehavior<CannonActor.Command> {
    public interface Command extends CborSerializable {}

    public record StartAlgorithm(ActorRef<ConnectionActor.Command> sender, int nodeId) implements Command {}
    public record AddAMatrixToQueue(IterationMatrix matrix) implements Command {}
    public record AddBMatrixToQueue(IterationMatrix matrix) implements Command {}
    private static final class LoadMatrices implements Command {}
    public static final class Stop implements Command {}

    private final ActorRef<ConnectionActor.Command> connectionActor;
    private int nodeId;
    private final int totalWorkers;
    private final int nodesInRow;
    private int currentIteration = -1;
    private Matrix C;
    private long startTime;
    private final int matrixSize;
    private final String loadMatrixDirectory;
    private final String saveMatrixDirectory;
    private Queue<IterationMatrix> aMatrixQueue = new LinkedList<>();
    private Queue<IterationMatrix> bMatrixQueue = new LinkedList<>();

    private CannonActor(
            ActorContext<Command> context,
            AppConfiguration config,
            ActorRef<ConnectionActor.Command> connectionActor
    ) {
        super(context);
        this.connectionActor = connectionActor;
        this.totalWorkers = config.getWorkers();
        this.nodesInRow = (int) Math.sqrt(totalWorkers);
        this.matrixSize = config.getMatrixSize();
        this.loadMatrixDirectory = config.getMatrixLoadDirectoryPath();
        this.saveMatrixDirectory = config.getMatrixSaveDirectoryPath();
    }

    public static Behavior<Command> create(AppConfiguration config) {
        return Behaviors.setup(context -> {
            ActorRef<ConnectionActor.Command> connectionActor =
                    context.spawn(ConnectionActor.create(context.getSelf(), config.getWorkers()), "Connector");

            return new CannonActor(context, config, connectionActor);
        });
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartAlgorithm.class, this::onStartAlgorithm)
                .onMessage(AddAMatrixToQueue.class, this::onAddAMatrixToQueue)
                .onMessage(AddBMatrixToQueue.class, this::onAddBMatrixToQueue)
                .onMessage(LoadMatrices.class, this::onLoadMatrices)
                .onMessage(Stop.class, this::onStop)
                .build();
    }

    private Behavior<Command> onStartAlgorithm(StartAlgorithm message) {
        if (!message.sender.equals(connectionActor)) {
            getContext().getLog().error("CannonActor {} received StartAlgorithm from not child Connector!", nodeId);
            getContext().getSelf().tell(new Stop());

            return Behaviors.same();
        }
        this.nodeId = message.nodeId;
        getContext().getSelf().tell(new LoadMatrices());

        return Behaviors.same();
    }

    private Behavior<Command> onAddAMatrixToQueue(AddAMatrixToQueue message) {
        aMatrixQueue.add(message.matrix);
        processCannonStep();

        return Behaviors.same();
    }

    private Behavior<Command> onAddBMatrixToQueue(AddBMatrixToQueue message) {
        bMatrixQueue.add(message.matrix);
        processCannonStep();

        return Behaviors.same();
    }

    private Behavior<Command> onLoadMatrices(LoadMatrices message) {
        getContext().spawn(MatrixReaderActor.create(getContext().getSelf(), loadMatrixDirectory, matrixSize, nodeId, nodesInRow), "MatrixAReader")
                .tell(new MatrixReaderActor.ReadAMatrixFromFile());

        getContext().spawn(MatrixReaderActor.create(getContext().getSelf(), loadMatrixDirectory, matrixSize, nodeId, nodesInRow), "MatrixBReader")
                .tell(new MatrixReaderActor.ReadBMatrixFromFile());

        return Behaviors.same();
    }

    private Behavior<Command> onStop(Stop message) {
        getContext().getLog().info("CannonActor {} stopping...", nodeId);
        getContext().getSystem().terminate();

        return Behaviors.stopped();
    }

    private void processCannonStep() {
        IterationMatrix iterationMatrixA = findAMatrixInQueue();
        if (iterationMatrixA == null) {
            return;
        }

        IterationMatrix iterationMatrixB = findBMatrixInQueue();
        if (iterationMatrixB == null) {
            this.aMatrixQueue.add(iterationMatrixA);

            return;
        }

        Matrix A = iterationMatrixA.getMatrix();
        Matrix B = iterationMatrixB.getMatrix();

        if (++this.currentIteration == 0) {
            this.C = new Matrix(A.getCols(), B.getRows(), 0);
            getContext().getLog().info("CannonActor {} loaded matrices, starting calculating...", nodeId);
            this.startTime = System.nanoTime();

            int leftShiftNodeIndex = Cannon.getLeftShiftReceiverIndexInFirstStep(this.nodeId, this.nodesInRow);
            int upShiftNodeIndex = Cannon.getUpperShiftReceiverIndexInFirstStep(this.nodeId, this.nodesInRow);

            this.connectionActor.tell(new ConnectionActor.SendAMatrixToNode(new IterationMatrix(A, this.currentIteration), leftShiftNodeIndex));
            this.connectionActor.tell(new ConnectionActor.SendBMatrixToNode(new IterationMatrix(B, this.currentIteration), upShiftNodeIndex));

            processCannonStep();
        } else if (this.currentIteration < this.nodesInRow) {
            C.addMultiply(A, B);
            int leftShiftNodeIndex = Cannon.getLeftShiftByOneReceiverIndex(this.nodeId, this.nodesInRow);
            int upShiftNodeIndex = Cannon.getUpperShiftByOneReceiverIndex(this.nodeId, this.nodesInRow);

            this.connectionActor.tell(new ConnectionActor.SendAMatrixToNode(new IterationMatrix(A, this.currentIteration), leftShiftNodeIndex));
            this.connectionActor.tell(new ConnectionActor.SendBMatrixToNode(new IterationMatrix(B, this.currentIteration), upShiftNodeIndex));

            processCannonStep();
        } else {
            C.addMultiply(A, B);
            getContext().getLog().info("CannonActor {} Time: {} ", nodeId, (System.nanoTime() - startTime) / 1e9);

            getContext().spawn(MatrixWriterActor.create(getContext().getSelf(), this.saveMatrixDirectory), "MatrixWriter")
                    .tell(new MatrixWriterActor.SaveMatrixToFile(this.C, this.nodeId));
        }
    }

    private IterationMatrix findAMatrixInQueue() {
        final int inputQueueSize = this.aMatrixQueue.size();
        Queue<IterationMatrix> tmpQueue = new LinkedList<>();
        for (int i = 0; i < inputQueueSize; ++i) {
            IterationMatrix tmp = this.aMatrixQueue.remove();

            if (tmp.isFromCurrentIteration(this.currentIteration)) {
                final int tmpSize = tmpQueue.size();
                for (int j = 0; j < tmpSize; ++j) {
                    IterationMatrix reAdd = tmpQueue.remove();
                    this.aMatrixQueue.add(reAdd);
                }

                return tmp;
            } else {
                tmpQueue.add(tmp);
            }
        }

        final int tmpSize = tmpQueue.size();
        for (int j = 0; j < tmpSize; ++j) {
            IterationMatrix reAdd = tmpQueue.remove();
            this.aMatrixQueue.add(reAdd);
        }

        return null;
    }

    private IterationMatrix findBMatrixInQueue() {
        final int inputQueueSize = this.bMatrixQueue.size();
        Queue<IterationMatrix> tmpQueue = new LinkedList<>();
        for (int i = 0; i < inputQueueSize; ++i) {
            IterationMatrix tmp = this.bMatrixQueue.remove();

            if (tmp.isFromCurrentIteration(this.currentIteration)) {
                final int tmpSize = tmpQueue.size();
                for (int j = 0; j < tmpSize; ++j) {
                    IterationMatrix reAdd = tmpQueue.remove();
                    this.bMatrixQueue.add(reAdd);
                }

                return tmp;
            } else {
                tmpQueue.add(tmp);
            }
        }

        final int tmpSize = tmpQueue.size();
        for (int j = 0; j < tmpSize; ++j) {
            IterationMatrix reAdd = tmpQueue.remove();
            this.bMatrixQueue.add(reAdd);
        }

        return null;
    }
}
