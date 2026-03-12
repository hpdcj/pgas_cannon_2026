package cannon.cluster.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import cannon.cluster.CborSerializable;
import cannon.cluster.other.IterationMatrix;
import cannon.cluster.other.MatrixReader;

import java.io.FileNotFoundException;

public class MatrixReaderActor extends AbstractBehavior<MatrixReaderActor.Command> {
    public interface Command extends CborSerializable {}

    public record ReadAMatrixFromFile() implements Command {}
    public record ReadBMatrixFromFile() implements Command {}

    private final ActorRef<CannonActor.Command> parent;
    private final String fileDirectory;
    private final int matrixSize;
    private final int nodeId;
    private final int nodesInRow;

    private MatrixReaderActor(ActorContext<Command> context, ActorRef<CannonActor.Command> parent, String fileDirectory, int matrixSize, int nodeId, int nodesInRow) {
        super(context);
        this.parent = parent;
        this.fileDirectory = fileDirectory;
	this.matrixSize = matrixSize;
        this.nodeId = nodeId;
        this.nodesInRow = nodesInRow;
    }

    public static Behavior<Command> create(ActorRef<CannonActor.Command> parent, String fileDirectory, int matrixSize, int nodeId, int nodesInRow) {
        return Behaviors.setup(context -> new MatrixReaderActor(context, parent, fileDirectory, matrixSize, nodeId, nodesInRow));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReadAMatrixFromFile.class, this::onReadAMatrix)
                .onMessage(ReadBMatrixFromFile.class, this::onReadBMatrix)
                .build();
    }

    private Behavior<Command> onReadAMatrix(ReadAMatrixFromFile message) throws FileNotFoundException {
        parent.tell(new CannonActor.AddAMatrixToQueue(
                new IterationMatrix(
                        MatrixReader.readAndSplitSquareMatrixFromFile(String.format("%s/A%d.txt", fileDirectory, matrixSize), nodeId, nodesInRow),
                        -1
                )
        ));

        return Behaviors.stopped();
    }

    private Behavior<Command> onReadBMatrix(ReadBMatrixFromFile message) throws FileNotFoundException {
        parent.tell(new CannonActor.AddBMatrixToQueue(
                new IterationMatrix(
                        MatrixReader.readAndSplitSquareMatrixFromFile(String.format("%s/B%d.txt", fileDirectory, matrixSize), nodeId, nodesInRow),
                        -1
                )
        ));

        return Behaviors.stopped();
    }
}
