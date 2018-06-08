package surfstore;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import com.google.protobuf.ByteString;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    // for distributed test
    ManagedChannel metadataChannelf1;
    MetadataStoreGrpc.MetadataStoreBlockingStub metadataStubf1;
    ManagedChannel metadataChannelf2;
    MetadataStoreGrpc.MetadataStoreBlockingStub metadataStubf2;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum()))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    // for distributed test
    public void initFollower(ConfigReader config) {
        this.metadataChannelf1 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
                .usePlaintext(true).build();
        this.metadataStubf1 = MetadataStoreGrpc.newBlockingStub(metadataChannelf1);
        this.metadataChannelf2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
                .usePlaintext(true).build();
        this.metadataStubf2 = MetadataStoreGrpc.newBlockingStub(metadataChannelf2);
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    private void go(Namespace args) {
	metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        String op = args.getString("operation");
        String fn = args.getString("file_name");

        try {
            File configf = new File(args.getString("config_file"));
            ConfigReader config = new ConfigReader(configf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        switch(op) {
            case "upload":
              upload(fn);
              break;
            case "download":
              download(fn, args.getString("dir"));
              break;
            case "delete":
              delete(fn);
              break;
            case "getversion":
              getversion(fn);
              break;
            case "getversion_f":
              initFollower(config);
              getversion_f(fn, args.getInt("follower_number"));
              break;
            case "crash":
              initFollower(config);
              crash(args.getInt("follower_number"));
              break;
            case "restore":
              initFollower(config);
              restore(args.getInt("follower_number"));
              break;
            default:
              break;
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("operation").type(String.class)
                .help("Operation the user wants to do");
        parser.addArgument("file_name").type(String.class)
                .help("File the user wants to operate");
        
        if (args.length > 3) {
          parser.addArgument("dir").type(String.class)
                .help("Where the user wants to download").setDefault("/");
        }

        if (args.length > 4) {
          parser.addArgument("follower_number").type(Integer.class);
        }
        
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

        Client client = new Client(config);

        
        try {
        	client.go(c_args);
		//client.distributedTest(config);
        } finally {
            client.shutdown();
        }
    }

    public void upload(String fn) {
        File f = new File(fn);
        if (!f.exists() || f.isDirectory()) {
            System.err.println("Not Found");
            return;
        }
        int index = fn.lastIndexOf('/');
        if (index >= 0)
            fn = fn.substring(index + 1);
        byte[] buffer = new byte[4096];
        int read = 0;
	HashMap<String, ByteString> map = new HashMap<>();

        FileInfo readRequest = FileInfo.newBuilder().setFilename(fn).build();
        FileInfo readResponse = metadataStub.readFile(readRequest);

        ArrayList<String> hashlist = new ArrayList<>();

        MessageDigest digest = null;
	try {
		digest = MessageDigest.getInstance("SHA-256");		
	} catch (NoSuchAlgorithmException e) {
		e.printStackTrace();
		System.exit(1);
	}

        try {
	        FileInputStream fis = new FileInputStream(f);

        	while ( (read = fis.read(buffer)) > 0 ) {
	            byte[] hash = digest.digest(buffer);
        	    String encoded = Base64.getEncoder().encodeToString(hash);
	            hashlist.add(encoded);
	            map.put(encoded, ByteString.copyFrom(buffer, 0, read));
		    Arrays.fill(buffer, (byte)0);
	        }

		fis.close();
	} catch (Exception e) {
		e.printStackTrace();
		System.exit(1);
        }

        FileInfo modifyRequest = FileInfo.newBuilder().setFilename(fn).setVersion(readResponse.getVersion() + 1).addAllBlocklist(hashlist).build();

	WriteResult modifyResponse = metadataStub.modifyFile(modifyRequest);
	WriteResult.Result res = modifyResponse.getResult();

        while (res != WriteResult.Result.OK) {
            if (res == WriteResult.Result.OLD_VERSION) {
                modifyRequest = FileInfo.newBuilder().setFilename(fn).setVersion(modifyResponse.getCurrentVersion() + 1).addAllBlocklist(hashlist).build();
	        modifyResponse = metadataStub.modifyFile(modifyRequest);
	        res = modifyResponse.getResult();
            }
            else if (res == WriteResult.Result.MISSING_BLOCKS) {
                List<String> missing_blocks = modifyResponse.getMissingBlocksList();
                for (String s : missing_blocks) {
                    Block storeRequest = Block.newBuilder().setHash(s).setData(map.get(s)).build();
                    blockStub.storeBlock(storeRequest);
                }

	        modifyResponse = metadataStub.modifyFile(modifyRequest);
	        res = modifyResponse.getResult();
            }
            else {
                System.err.println("upload can't be called on follower!");
                return;
            }
        }

        System.out.println("OK");

    }

    public void download(String fn, String dir) {
        File f = new File(dir + "/" + fn);
        byte[] buffer = new byte[4096];
        int read = 0;
	HashMap<String, ByteString> map = new HashMap<>();

        MessageDigest digest = null;
	try {
		digest = MessageDigest.getInstance("SHA-256");		
	} catch (NoSuchAlgorithmException e) {
		e.printStackTrace();
		System.exit(1);
	}

        FileInfo readRequest = FileInfo.newBuilder().setFilename(fn).build();
        FileInfo readResponse = metadataStub.readFile(readRequest);
        List<String> blocklist = readResponse.getBlocklistList();

        if (readResponse.getVersion() == 0 || (blocklist.size() == 1 && blocklist.get(0).equals("0"))) {
            System.out.println("Not Found");
            return;
        }

        File d = new File(dir);
        for (File each : d.listFiles()) {
        	try {
	        	FileInputStream fis = new FileInputStream(each);

        		while ( (read = fis.read(buffer)) > 0 ) {
	        	    byte[] hash = digest.digest(buffer);
	        	    String encoded = Base64.getEncoder().encodeToString(hash);
                            if (!map.containsKey(encoded))
  	        	        map.put(encoded, ByteString.copyFrom(buffer));
		        }
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
       		}
        }

        if (f.exists())
          f.delete();

	try {
	    FileOutputStream stream = new FileOutputStream(dir + "/" + fn);
            for (String s : blocklist) {
		if (!map.containsKey(s)) {
        		Block getRequest = Block.newBuilder().setHash(s).build();
		        Block getResponse = blockStub.getBlock(getRequest);
			map.put(s, getResponse.getData());
		}
	        stream.write(map.get(s).toByteArray());
            }
	    stream.close();
	}
        catch (Exception e) {
	    e.printStackTrace();
	    System.exit(1);
        }
        System.out.println("OK");
    }

    public void delete(String fn) {

        int index = fn.lastIndexOf('/');
        if (index >= 0)
            fn = fn.substring(index + 1);
        FileInfo readRequest = FileInfo.newBuilder().setFilename(fn).build();
        FileInfo readResponse = metadataStub.readFile(readRequest);
        List<String> blocklist = readResponse.getBlocklistList();

        if (readResponse.getVersion() == 0 || (blocklist.size() == 1 && blocklist.get(0).equals("0"))) {
            System.out.println("Not Found");
            return;
        }

        FileInfo deleteRequest = FileInfo.newBuilder().setFilename(fn).setVersion(readResponse.getVersion() + 1).build();

	WriteResult deleteResponse = metadataStub.deleteFile(deleteRequest);
	WriteResult.Result res = deleteResponse.getResult();

        while (res != WriteResult.Result.OK) {
            if (res == WriteResult.Result.OLD_VERSION) {
                deleteRequest = FileInfo.newBuilder().setFilename(fn).setVersion(deleteResponse.getCurrentVersion() + 1).build();
	        deleteResponse = metadataStub.deleteFile(deleteRequest);
	        res = deleteResponse.getResult();
	    }
            else {
                System.err.println("errors happen in delete!");
                return;
            }
	}

	System.out.println("OK");

    }

    public void getversion(String fn) {
        int index = fn.lastIndexOf('/');
        if (index >= 0)
            fn = fn.substring(index + 1);
        FileInfo request = FileInfo.newBuilder().setFilename(fn).build();
        FileInfo response = metadataStub.getVersion(request);
        System.out.println(response.getVersion());
    }

    // for distributed test
    public void distributedTest(ConfigReader config) {

        initFollower(config);

	metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

	metadataStubf1.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata follower1 server");

	metadataStubf2.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata follower2 server");

        upload("./testfiles/set1/test1.txt");
        upload("./testfiles/set1/test2.txt");
        metadataStubf1.crash(Empty.newBuilder().build());
        upload("./testfiles/set1/test3.txt");
        upload("./testfiles/set2/test1.txt");
        upload("./testfiles/set3/test1.txt");
        FileInfo request = FileInfo.newBuilder().setFilename("test1.txt").build();
        FileInfo response = metadataStubf1.getVersion(request);
        System.out.println(response.getVersion());
        metadataStubf1.restore(Empty.newBuilder().build());
        
        try {
            Thread.sleep(5000);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
        request = FileInfo.newBuilder().setFilename("test1.txt").build();
        response = metadataStubf1.getVersion(request);
        System.out.println(response.getVersion());
    }

    public void getversion_f(String fn, int follower) {
        if (follower == 1) {
	    metadataStubf1.ping(Empty.newBuilder().build());
            logger.info("Successfully pinged the Metadata follower1 server");
            FileInfo request = FileInfo.newBuilder().setFilename(fn).build();
            FileInfo response = metadataStubf1.getVersion(request);
            System.out.println(response.getVersion());
        }
        else {
	    metadataStubf2.ping(Empty.newBuilder().build());
            logger.info("Successfully pinged the Metadata follower2 server");
            FileInfo request = FileInfo.newBuilder().setFilename(fn).build();
            FileInfo response = metadataStubf2.getVersion(request);
            System.out.println(response.getVersion());
        }
    }

    public void crash(int follower) {
        if (follower == 1) {
      	    metadataStubf1.ping(Empty.newBuilder().build());
            logger.info("Successfully pinged the Metadata follower1 server");
            metadataStubf1.crash(Empty.newBuilder().build());
        }
        else {
	    metadataStubf2.ping(Empty.newBuilder().build());
            logger.info("Successfully pinged the Metadata follower2 server");
            metadataStubf2.crash(Empty.newBuilder().build());
        }
    }

    public void restore(int follower) {
        if (follower == 1) {
	    metadataStubf1.ping(Empty.newBuilder().build());
            logger.info("Successfully pinged the Metadata follower1 server");
            metadataStubf1.restore(Empty.newBuilder().build());
        }
        else {
	    metadataStubf2.ping(Empty.newBuilder().build());
            logger.info("Successfully pinged the Metadata follower2 server");
            metadataStubf2.restore(Empty.newBuilder().build());
        }
    }
}
