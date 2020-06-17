package io.confluent.ps.abraham.leal;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.Set;

@Path("streams-layer")
public class stateQuerier {

    private Server jettyServer;
    private final KafkaStreams streams;
    private Logger logger = Logger.getLogger(stateQuerier.class);

    stateQuerier(final KafkaStreams streams) {
        this.streams = streams;
    }

    @GET
    @Path("/healthcheck")
    public Response getHealth(){

        Response check = Response.serverError().build();

        if(streams.state().isRunningOrRebalancing()){
            Set<ThreadMetadata> myThreadsStatus = streams.localThreadsMetadata();
            check = Response.ok().build();
            logger.debug("Streams app is running");
            for (ThreadMetadata x : myThreadsStatus){
                if (!x.threadState().equals("RUNNING")){
                    logger.debug("At least one thread in the Streams app is dead, returning bad response.");
                    check = Response.serverError().build();
                }
            }
        }

        return check;

    }

    void start() throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        
        ResourceConfig rc = new ResourceConfig();
        rc.register(this);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer = new Server(7000);
        jettyServer.setHandler(context);

        logger.info("Starting jetty server");
        jettyServer.start();
    }

    void stop() {
        try{
            jettyServer.stop();
        } catch(Exception e){
            logger.info("No jetty server can be stopped since none has been initialized");
        }
    }

}
