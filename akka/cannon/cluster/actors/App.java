package cannon.cluster.actors;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cannon.cluster.other.AppConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class App {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("cannon");
        int workers = config.getInt("cannon.workers");
        int sqrt = (int) Math.sqrt(workers);
        if (workers < 1 || (sqrt * sqrt) != workers) {
            throw new IllegalArgumentException("Number of places has to be the square of any unsigned integer, other than 0");
        }

        if (args.length == 1) {
            startup(Integer.parseInt(args[0]));
        } else if (args.length == 0) {
            startup(25251);

            for (int leftWorkers = 1; leftWorkers < workers; leftWorkers++) {
                startup(0);
            }
        } else {
            throw new IllegalArgumentException("Usage: port");
        }
    }

    private static void startup(int port) {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("akka.remote.artery.canonical.port", port);

        Config config = ConfigFactory.parseMap(overrides)
                .withFallback(ConfigFactory.load("cannon"));

        ActorSystem<Void> system = ActorSystem.create(RootBehavior.create(), "NodeWorkerSystem", config);
    }

    private static class RootBehavior extends AbstractBehavior<Void> {
        private RootBehavior(ActorContext<Void> context) {
            super(context);

            int workers = context.getSystem().settings().config().getInt("cannon.workers");
            int matrixSize = context.getSystem().settings().config().getInt("cannon.matrixSize");
            String loadMatrixDirectory = context.getSystem().settings().config().getString("cannon.matrixLoadDirectoryPath");
            String saveMatrixDirectory = context.getSystem().settings().config().getString("cannon.matrixSaveDirectoryPath");

            context.spawn(
                    CannonActor.create(new AppConfiguration(workers, matrixSize, loadMatrixDirectory, saveMatrixDirectory)),
                    UUID.randomUUID().toString()
            );
        }

        static Behavior<Void> create() {
            return Behaviors.setup(RootBehavior::new);
        }

        @Override
        public Receive<Void> createReceive() {
            return newReceiveBuilder().build();
        }
    }
}
