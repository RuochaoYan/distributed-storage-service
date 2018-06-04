package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.*;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        protected Map<String, String[]> blocklistMap;
        protected Map<String, Integer> versionMap;
        protected Set<String> blockSet;
        protected boolean isLeader;

        public MetadataStoreImpl(){
            super();
            this.blocklistMap = new HashMap<String, String[]>();
            this.versionMap = new HashMap<String, Integer>();
            this.blockSet = new HashSet<String>();
            this.isLeader = true;
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!
        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            logger.info("Reading file with name " + request.getFilename());

            FileInfo.Builder builder = FileInfo.newBuilder();
            if(!blocklistMap.containsKey(request.getFilename())){
                builder.setVersion(0);
            }
            else{
                String[] blocklist = blocklistMap.get(request.getFilename());
                int version = versionMap.get(request.getVersion());
                for(String block : blocklist){
                    builder.addBlocklist(block);
                }
                builder.setVersion(version);
            }
            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();  
        }

        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            String fileName = request.getFilename();
            logger.info("Modifying file with name " + fileName);

            
            WriteResult.Builder builder = WriteResult.newBuilder();

            if(!blocklistMap.containsKey(fileName)){
                blocklistMap.put(fileName, new String[0]);
                versionMap.put(fileName, 0);
            }
            int oldVersion = versionMap.get(fileName);
            String[] oldBlocklist = blocklistMap.get(fileName);
            if(request.getVersion() - oldVersion == 1){
                builder.setCurrentVersion(request.getVersion());
                builder.setResultValue(0);
                int missCount = 0;
                String[] newBlocklist = request.getBlocklistList().toArray(new String[request.getBlocklistList().size()]);
                for(String block : newBlocklist){
                    if(!blockSet.contains(block)){
                        missCount++;
                        builder.addMissingBlocks(block);
                        blockSet.add(block);
                    }
                }
                if(missCount != 0)
                    builder.setResultValue(2);
                blocklistMap.put(fileName, newBlocklist);
                versionMap.put(fileName, request.getVersion());
            }
            else{
                builder.setResultValue(1);
                builder.setCurrentVersion(oldVersion);
                logger.info("Modification failed!");
            }

            WriteResult response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            String fileName = request.getFilename();
            logger.info("Deleting file with name " + fileName);

            WriteResult.Builder builder = WriteResult.newBuilder();
            // delete a file that doesn't exist?

            int oldVersion = versionMap.get(fileName);
            if(request.getVersion() - oldVersion == 1){
                builder.setResultValue(0);
                builder.setCurrentVersion(0);
                blocklistMap.put(fileName, new String[0]);
                versionMap.put(fileName, request.getVersion());
            }
            else{
                builder.setResultValue(1);
                builder.setCurrentVersion(oldVersion);
                logger.info("Deletion failed!");
            }

            WriteResult response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();            
        }

        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isLeader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();            
        }

        // @Override
        // public void crash(surfstore.SurfStoreBasic.Empty request,
        //     io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
        //     responseObserver.onNext(response);
        //     responseObserver.onCompleted();            
        // }

        // @Override
        // public void restore(surfstore.SurfStoreBasic.Empty request,
        //     io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
        //     responseObserver.onNext(response);
        //     responseObserver.onCompleted();
        // }

        // @Override
        // public void isCrashed(surfstore.SurfStoreBasic.Empty request,
        //     io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
        //     responseObserver.onNext(response);
        //     responseObserver.onCompleted();
        // }

        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            String fileName = request.getFilename();
            logger.info("Getting version of file: " + fileName);
            FileInfo.Builder builder = FileInfo.newBuilder();
            int version = versionMap.get(fileName);
            builder.setVersion(version);
            builder.setFilename(fileName);
            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();            
        }

    }
}