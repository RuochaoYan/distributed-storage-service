package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
    protected boolean isLeader;

    public MetadataStore(ConfigReader config, boolean isLeader) {
    	this.config = config;
        this.isLeader = isLeader;
	}

	private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(config, isLeader))
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

        boolean isLeader = c_args.getInt("number") == config.getLeaderNum() ? true : false;

        final MetadataStore server = new MetadataStore(config, isLeader);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {
        protected Map<String, List<String>> blocklistMap;
        protected Map<String, Integer> versionMap;
        protected boolean isLeader;
        private final ManagedChannel blockChannel;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        private final ConfigReader config;
        protected List<String> logFilename;
        protected List<List<String>> logBlocklist;
        protected List<Integer> logVersion;
        protected boolean isCrashed;

        private final ManagedChannel followerMetadataChannel1;
        private final MetadataStoreGrpc.MetadataStoreBlockingStub followerMetadataStub1;
        private final ManagedChannel followerMetadataChannel2;
        private final MetadataStoreGrpc.MetadataStoreBlockingStub followerMetadataStub2;

        public MetadataStoreImpl(ConfigReader config, boolean isLeader){
            super();
            this.blocklistMap = new HashMap<String, List<String>>();
            this.versionMap = new HashMap<String, Integer>();
            this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
            this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
            this.config = config;
            this.isLeader = isLeader;
            this.logFilename = new LinkedList<String>();
            this.logBlocklist = new LinkedList<List<String>>();
            this.logVersion = new LinkedList<Integer>();
            this.isCrashed = false;

            this.followerMetadataChannel1 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
                .usePlaintext(true).build();
            this.followerMetadataStub1 = MetadataStoreGrpc.newBlockingStub(followerMetadataChannel1);
            this.followerMetadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
                .usePlaintext(true).build();
            this.followerMetadataStub2 = MetadataStoreGrpc.newBlockingStub(followerMetadataChannel2);
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
                int version = versionMap.get(request.getFilename());
                if(!isCrashed || isLeader){
                    List<String> blocklist = blocklistMap.get(request.getFilename());
                    for(String block : blocklist){
                        builder.addBlocklist(block);
                    }
                }
                builder.setVersion(version);
                builder.setFilename(request.getFilename());
            }
            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();  
        }

        public void handleLogAndCommit(String fileName, int version, List<String> blocklist){
            logFilename.add(fileName);
            logBlocklist.add(blocklist);
            logVersion.add(version);
            Log appendLogRequest = Log.newBuilder().setLogIndex(logFilename.size()-1).setFilename(fileName).addAllBlocklist(blocklist).setVersion(version).build();
            FollowerStatus logResponse1 = followerMetadataStub1.appendLog(appendLogRequest);
            FollowerStatus logResponse2 = followerMetadataStub2.appendLog(appendLogRequest);
            FollowerStatus.Result res1 = logResponse1.getResult();
            FollowerStatus.Result res2 = logResponse2.getResult();
            if(res1 == FollowerStatus.Result.LAG){
                int f1LatestLog = logResponse1.getLatestLog();
                for(int i = f1LatestLog+1; i < logFilename.size(); i++){
                    Log uptodateRequest = Log.newBuilder().setLogIndex(i).setFilename(logFilename.get(i)).addAllBlocklist(logBlocklist.get(i)).setVersion(logVersion.get(i)).build();
                    FollowerStatus uptodateResponse = followerMetadataStub1.appendLog(uptodateRequest);
                    if(uptodateResponse.getResult() == FollowerStatus.Result.OK){
                        Empty commitRequest = Empty.newBuilder().build();
                        followerMetadataStub1.commitOperation(commitRequest);
                    }
                    else
                        break;
                }
            }
            if(res2 == FollowerStatus.Result.LAG){
                int f2LatestLog = logResponse2.getLatestLog();
                for(int i = f2LatestLog+1; i < logFilename.size(); i++){
                    Log uptodateRequest = Log.newBuilder().setLogIndex(i).setFilename(logFilename.get(i)).addAllBlocklist(logBlocklist.get(i)).setVersion(logVersion.get(i)).build();
                    FollowerStatus uptodateResponse = followerMetadataStub2.appendLog(uptodateRequest);
                    if(uptodateResponse.getResult() != FollowerStatus.Result.OK){
                        Empty commitRequest = Empty.newBuilder().build();
                        followerMetadataStub2.commitOperation(commitRequest);
                    }
                    else
                        break;
                }
            }
            if(res1 == FollowerStatus.Result.OK || res2 == FollowerStatus.Result.OK){
                Empty commitRequest = Empty.newBuilder().build();
                followerMetadataStub1.commitOperation(commitRequest);
                followerMetadataStub2.commitOperation(commitRequest);

                blocklistMap.put(fileName, blocklist);
                versionMap.put(fileName, version);
            }
        }

        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            String fileName = request.getFilename();
            int lastSlash = fileName.lastIndexOf('/');
            if(lastSlash >= 0)
                fileName = fileName.substring(lastSlash + 1);
            logger.info("Modifying file with name " + fileName);
            
            WriteResult.Builder builder = WriteResult.newBuilder();
            if(isCrashed || !isLeader){
                builder.setResultValue(3);
            }
            else{
                if(!blocklistMap.containsKey(fileName)){
                    blocklistMap.put(fileName, new LinkedList<String>());
                    versionMap.put(fileName, 0);
                }
                int oldVersion = versionMap.get(fileName);
                List<String> oldBlocklist = blocklistMap.get(fileName);
                if(request.getVersion() - oldVersion == 1){
                    int missCount = 0;
                    List<String> newBlocklist = request.getBlocklistList();
                    for(String block : newBlocklist){
                        Block checkRequest = Block.newBuilder().setHash(block).build();
                        SimpleAnswer checkResponse = blockStub.hasBlock(checkRequest);
                        if(!checkResponse.getAnswer()){
                            missCount++;
                            builder.addMissingBlocks(block);
                        }
                    }
                    if(missCount != 0){
                        builder.setCurrentVersion(oldVersion);
                        builder.setResultValue(2);
                    }
                    else{
                        builder.setResultValue(0);
                        builder.setCurrentVersion(request.getVersion());
                        // update the log and send the log to the followers
                        handleLogAndCommit(fileName, request.getVersion(), newBlocklist);
                    }

                }
                else{
                    builder.setResultValue(1);
                    builder.setCurrentVersion(oldVersion);
                    logger.info("Modification failed!");
                }                
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

            if(isCrashed || !isLeader){
                builder.setResultValue(3);
            }
            else{
                int oldVersion = versionMap.get(fileName);
                if(request.getVersion() - oldVersion == 1){
                    builder.setResultValue(0);
                    builder.setCurrentVersion(0);

                    

                    List<String> newBlocklist = new LinkedList<>();
                    newBlocklist.add("0");
                    // send a log to the followers
                    handleLogAndCommit(fileName, request.getVersion(), newBlocklist);
                }
                else{
                    builder.setResultValue(1);
                    builder.setCurrentVersion(oldVersion);
                    logger.info("Deletion failed!");
                }                
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

        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            isCrashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();            
        }

        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isCrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            String fileName = request.getFilename();
            logger.info("Getting version of file: " + fileName);
            FileInfo.Builder builder = FileInfo.newBuilder();
            int version = 0;
            if(blocklistMap.containsKey(fileName)){
                version = versionMap.get(fileName);
            }
            builder.setVersion(version);
            builder.setFilename(fileName);
            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();            
        }

        @Override
        public void appendLog(surfstore.SurfStoreBasic.Log request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FollowerStatus> responseObserver) {
            FollowerStatus.Builder builder = FollowerStatus.newBuilder();
            if(isCrashed){
                builder.setResultValue(1);
            }
            else if(logFilename.size() == request.getLogIndex()){
                logFilename.add(request.getFilename());
                logBlocklist.add(request.getBlocklistList());
                logVersion.add(request.getVersion());
                builder.setResultValue(0);
            }
            else{
                builder.setResultValue(2);
                builder.setLatestLog(logFilename.size()-1);
            }
            FollowerStatus response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();               
        }

        @Override
        public void commitOperation(surfstore.SurfStoreBasic.Empty request,
            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            blocklistMap.put(logFilename.get(logFilename.size()-1), logBlocklist.get(logBlocklist.size()-1));
            versionMap.put(logFilename.get(logFilename.size()-1), logVersion.get(logVersion.size()-1));
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

    }
}