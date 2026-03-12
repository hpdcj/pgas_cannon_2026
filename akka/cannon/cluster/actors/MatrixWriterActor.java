package cannon.cluster.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import cannon.cluster.CborSerializable;
import cannon.cluster.other.Matrix;
import cannon.cluster.other.MatrixWriter;

import java.io.FileNotFoundException;

public class MatrixWriterActor extends AbstractBehavior<MatrixWriterActor.Command> {
    public interface Command extends CborSerializable {}

    public record SaveMatrixToFile(Matrix matrix, int nodeId) implements Command {}

    private final ActorRef<CannonActor.Command> parent;
    private final String saveDirectory;

    private MatrixWriterActor(ActorContext<Command> context, ActorRef<CannonActor.Command> parent, String saveDirectory) {
        super(context);
        this.parent = parent;
        this.saveDirectory = saveDirectory;
    }

    public static Behavior<Command> create(ActorRef<CannonActor.Command> parent, String saveDirectory) {
        return Behaviors.setup(context -> new MatrixWriterActor(context, parent, saveDirectory));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(SaveMatrixToFile.class, this::onSaveMatrixToFile)
                .build();
    }

    private Behavior<Command> onSaveMatrixToFile(SaveMatrixToFile message) throws FileNotFoundException {
        MatrixWriter.saveMatrixPart(message.matrix, message.nodeId, this.saveDirectory);

        parent.tell(new CannonActor.Stop());

        return Behaviors.stopped();
    }
}
