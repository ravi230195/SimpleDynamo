package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	static final int SERVER_PORT = 10000;
	Boolean REPLICATE_KEY = Boolean.FALSE;
	Boolean SINGLE_NODE = false;
	static String myPort = "0";
	static String portStr = "0";
	ContentValues contentValues = new ContentValues();
	Uri CONTENT_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	private ContentResolver contentResolver;
	ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	Lock readLock = readWriteLock.readLock();
	Lock writeLock = readWriteLock.writeLock();
	Context context;
	String myNodeId = "";
	static ArrayList<String> chordRingPorts = new ArrayList<String>();
	static TreeMap<String, String> chordRingNodes = new TreeMap<String, String>();
	static ArrayList<String> myKeys = new ArrayList<String>();
	static ConcurrentHashMap<String, String> myNodeDetails = new ConcurrentHashMap<String, String>();
	static HashMap<String, String[]> portsSorted = new HashMap<String, String[]>();
	static HashMap<String, String[]> predecessors = new HashMap<String, String[]>();
	static ArrayList<String> keysReplicated = new ArrayList<String>();
	static HashMap<String, ArrayList<String> > keysReplicatedMap = new HashMap<String, ArrayList<String>>();
	Object INSERT_LOCK = new Object();

	// Function to hash your own port number to get the new_node_id
	String getNodeId(String port){
		String portHashed = "";
		{
			try {
				portHashed = genHash(port);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		String nodeId = portHashed;
		return nodeId;
	}

	// Function to hash fileKey to get the fileKeyHashed
	String getKeyId(String Key){
		String keyHashed = "";
		{
			try {
				keyHashed = genHash(Key);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		String keyId = keyHashed;
		return keyId;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.e(TAG, "In delete: entered");
		// Delete in your device and forward the message to successor
		if(!selection.equals("*") && !selection.equals("@")){
			// This block of code will delete only one key
			Log.e(TAG, "In delete for more than 1 node in chord: Key to be deleted: "+selection);
			if(!myKeys.contains(selection) && !keysReplicated.contains(selection)){
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETE_KEY", myPort, myNodeId, selection);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETE_KEY_REPLICATION", myPort, myNodeId, selection);

			}else if(myKeys.contains(selection)) {
				String readFileName = selection;
				Log.e(TAG, readFileName);
				// Delete file in context
				this.getContext().deleteFile(readFileName);
				myKeys.remove(readFileName);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETE_KEY_REPLICATION", myPort, myNodeId, selection);

			}else if(keysReplicated.contains(selection)){
				String readFileName = selection;
				Log.e(TAG, readFileName);
				// Delete file in context
				this.getContext().deleteFile(readFileName);
				keysReplicated.remove(readFileName);
			}
			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DELETE_KEY_REPLICATION", myPort, myNodeId, selection);

		}else if(selection.equals("@") || selection.equals("*") && SINGLE_NODE){
			// This block of code will retrieve all keys in local i.e. LDump
			Iterator<String> it = myKeys.iterator();
			while (it.hasNext()){
				this.getContext().deleteFile(it.next());
				it.remove();
			}
			// Remove keys that are replicated
			it = keysReplicated.iterator();
			while (it.hasNext()){
				this.getContext().deleteFile(it.next());
				it.remove();
			}
		}else if(selection.equals("*")){
			// This block of code will retrieve all keys in local and send request to successor for keys i.e. GDump
			Iterator<String> it = myKeys.iterator();
			while (it.hasNext()){
				this.getContext().deleteFile(it.next());
				it.remove();
			}
			// Remove keys that are replicated
			it = keysReplicated.iterator();
			while (it.hasNext()){
				this.getContext().deleteFile(it.next());
				it.remove();
			}
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Delete", myPort, myNodeId);
		}
		return 1;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		Log.e(TAG, "In insert, entered");
		// Find your own port
		context=getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
			// Get key and val from the request
			String key = (String) values.get("key");
			String val = (String) values.get("value");
			// Hash the key. We don't need to hash the value.
			String keyHashed = getKeyId(key);
			Log.e(TAG, "||||||||||||||||||||||||||||||||||||||  " + key);
			if (myNodeId.compareTo(myNodeDetails.get("myPredecessor")) > 0 && (keyHashed.compareTo(getNodeId(myNodeDetails.get("myPredecessorPort"))) > 0 && keyHashed.compareTo(myNodeId) <= 0)) {
				Log.e(TAG, "In insert, entered own key condition");
				// Current key belongs to me on chord ring, because key > pred and key < succ.
				String fileName = key;
				String fileContent = val;
				FileOutputStream fileOutputStream;
				try {
					fileOutputStream = getContext().openFileOutput(fileName, getContext().MODE_PRIVATE);
					fileOutputStream.write(fileContent.getBytes());
					fileOutputStream.close();
					Log.e(TAG, "In Insert, File created, key inserted: " + key + "in: " + myPort);
					myKeys.add(key);
					// Send to successor for replication
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REPLICATE_KEY", myPort, myNodeId, key, val);
				} catch (Exception e) {
					Log.e(TAG, "In Insert, File write failed");
				}
			} else if (myNodeId.compareTo(myNodeDetails.get("myPredecessor")) < 0 && ((keyHashed.compareTo(myNodeDetails.get("myPredecessor")) > 0 && keyHashed.compareTo("0000000000000000000000000000000000000000") > 0)
					|| (keyHashed.compareTo("0000000000000000000000000000000000000000") >= 0 && keyHashed.compareTo(myNodeId) <= 0))) {
				Log.e(TAG, "In insert, entered own key condition: corner case");
				// Current key belongs to me on chord ring, because key > pred and key < succ and corner case
				String fileName = key;
				String fileContent = val;
				FileOutputStream fileOutputStream;
				try {
					fileOutputStream = getContext().openFileOutput(fileName, getContext().MODE_PRIVATE);
					fileOutputStream.write(fileContent.getBytes());
					fileOutputStream.close();
					Log.e(TAG, "In Insert, File created, key inserted: " + key + "in: " + myPort);
					myKeys.add(key);
					// Send to successor for replication
					//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "REPLICATE_KEY", myPort, myNodeId, key, val);
					String[] portsToReplicate = portsSorted.get(portStr);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT_KEY_REP", myPort, myNodeId, key, val, portsToReplicate[0]);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT_KEY_REP", myPort, myNodeId, key, val, portsToReplicate[1]);
				} catch (Exception e) {
					Log.e(TAG, "In Insert, File write failed");
				}
			}
			/*else if (REPLICATE_KEY == Boolean.TRUE) {
				//I'm the only node in the chord ring. So I will insert in my local storage.
				Log.e(TAG, "In insert, entered REPLICATE_KEY condition");
				String fileName = key;
				String fileContent = val;
				FileOutputStream fileOutputStream;
				try {
					fileOutputStream = getContext().openFileOutput(fileName, getContext().MODE_PRIVATE);
					fileOutputStream.write(fileContent.getBytes());
					fileOutputStream.close();
					Log.e(TAG, "In Insert, File created, replicated_key inserted: " + key + "in: " + myPort);
					keysReplicated.add(key);
					Log.e(TAG, "KEYS REPLICATED: " + keysReplicated.toArray().toString());

					String port = "";
					if ((keyHashed.compareTo(getNodeId("5562")) > 0 && keyHashed.compareTo(getNodeId("5556")) <= 0)) {
						port = "5556";
					} else if ((keyHashed.compareTo(getNodeId("5556")) > 0 && keyHashed.compareTo(getNodeId("5554")) <= 0)) {
						port = "5554";
					} else if ((keyHashed.compareTo(getNodeId("5554")) > 0 && keyHashed.compareTo(getNodeId("5558")) <= 0)) {
						port = "5558";
					} else if ((keyHashed.compareTo(getNodeId("5558")) > 0 && keyHashed.compareTo(getNodeId("5560")) <= 0)) {
						port = "5560";
					} else {
						port = "5562";
					}
					if(keysReplicatedMap.containsKey(port)){
						keysReplicatedMap.get(port).add(key);
//						ArrayList<String> temp2 = new ArrayList<String>();
//						temp2.addAll(keysReplicatedMap.get(port));
//						keysReplicatedMap.put(port, temp2);
					}else{
						ArrayList<String> temp2 = new ArrayList<String>();
						temp2.add(key);
						keysReplicatedMap.put(port, temp2);
					}
//					keysReplicatedMap;

				} catch (Exception e) {
					Log.e(TAG, "In Insert, File write failed");
				}
			}
			*/else {

				Log.e(TAG, "In insert, forwarding the request to key owner.");
				// Current key doesn't belong to me. Hence I will forward this request to key owner.
				// Condition to find owner
				// Condition to find owner
				String port = "";
				if ((keyHashed.compareTo(getNodeId("5562")) > 0 && keyHashed.compareTo(getNodeId("5556")) <= 0)) {
					port = "5556";
				} else if ((keyHashed.compareTo(getNodeId("5556")) > 0 && keyHashed.compareTo(getNodeId("5554")) <= 0)) {
					port = "5554";
				} else if ((keyHashed.compareTo(getNodeId("5554")) > 0 && keyHashed.compareTo(getNodeId("5558")) <= 0)) {
					port = "5558";
				} else if ((keyHashed.compareTo(getNodeId("5558")) > 0 && keyHashed.compareTo(getNodeId("5560")) <= 0)) {
					port = "5560";
				} else {
					port = "5562";
				}
				Log.e(TAG, "******************sending key: " + key + " to port**************** " + port);
				try {
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT_KEY", myPort, myNodeId, key, val, port);
					String[] portsToReplicate = portsSorted.get(port);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT_KEY_REP", myPort, myNodeId, key, val, portsToReplicate[0]);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INSERT_KEY_REP", myPort, myNodeId, key, val, portsToReplicate[1]);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
			return uri;
		}


	int givePorts(String portStr) {
		if (portStr.equals("5554")) {
			Log.e(TAG, "in onCreate, myPort = 11108");
			myNodeDetails.put("myPredecessor", getNodeId("5556"));
			myNodeDetails.put("myPredecessorPort", "5556");
			myNodeDetails.put("mySuccessor", getNodeId("5558"));
			myNodeDetails.put("mySuccessorPort", "5558");
		}
		if (portStr.equals("5556")) {
			myNodeDetails.put("myPredecessorPort", "5562");
			myNodeDetails.put("myPredecessor", getNodeId("5562"));
			myNodeDetails.put("mySuccessorPort", "5554");
			myNodeDetails.put("mySuccessor", getNodeId("5554"));
		}
		if (portStr.equals("5558")) {
			myNodeDetails.put("myPredecessorPort", "5554");
			myNodeDetails.put("myPredecessor", getNodeId("5554"));
			myNodeDetails.put("mySuccessorPort", "5560");
			myNodeDetails.put("mySuccessor", getNodeId("5560"));
		}
		if (portStr.equals("5560")) {
			myNodeDetails.put("myPredecessorPort", "5558");
			myNodeDetails.put("myPredecessor", getNodeId("5558"));
			myNodeDetails.put("mySuccessorPort", "5562");
			myNodeDetails.put("mySuccessor", getNodeId("5562"));
		}
		if (portStr.equals("5562")) {
			myNodeDetails.put("myPredecessorPort", "5560");
			myNodeDetails.put("myPredecessor", getNodeId("5560"));
			myNodeDetails.put("mySuccessorPort", "5556");
			myNodeDetails.put("mySuccessor", getNodeId("5556"));
		}
		return 1;
	}

	@Override
	public boolean onCreate () {
		// TODO Auto-generated method stub
		Log.e(TAG, "Already in ring: " + chordRingPorts);
		// Find your own port
		context = getContext();
		TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		// Get myNodeId by hashing the myPort
		myNodeId = getNodeId(portStr);
		myNodeDetails.put("myNodeId", myNodeId);
		myNodeDetails.put("myPort",portStr);
		givePorts(portStr);
		// To populate ports sorted
		portsSorted.put("5562", new String[]{"5556", "5554"});
		portsSorted.put("5556", new String[]{"5554", "5558"});
		portsSorted.put("5554", new String[]{"5558", "5560"});
		portsSorted.put("5558", new String[]{"5560", "5562"});
		portsSorted.put("5560", new String[]{"5562", "5556"});
		// To populate predecessors
		predecessors.put("5562", new String[]{"5560", "5558"});
		predecessors.put("5556", new String[]{"5562", "5560"});
		predecessors.put("5554", new String[]{"5556", "5562"});
		predecessors.put("5558", new String[]{"5554", "5556"});
		predecessors.put("5560", new String[]{"5558", "5554"});
		// Create ServerSocket to handle all requests and start server task
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			e.printStackTrace();

			myKeys.clear();
			keysReplicated.clear();
		}

		// OPEN A FILE WITH FILENAME RECOVERED ON_CREATE
		try {
			FileInputStream fileInputStream;
			fileInputStream = getContext().openFileInput("NODE_RECOVERY");
			Log.e(TAG, "NODE_RECOVERY file found.");
			new RecoveryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "NODE_RECOVERY", myPort, myNodeId);
//			String[] allfiles = getContext().fileList();
//			for(String file : allfiles){
//				getContext().deleteFile(file);
//			}
//          getContext().deleteFile("NODE_RECOVERY");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Log.e(TAG, "NODE_RECOVERY file not found. Exception raised: "+e.toString());
			FileOutputStream fileOutputStream;
			try {
				fileOutputStream = getContext().openFileOutput("NODE_RECOVERY", getContext().MODE_PRIVATE);
				fileOutputStream.write("NODE_RECOVERY".getBytes());
				fileOutputStream.close();
				Log.e(TAG, "ON CREATE: NODE_RECOVERY file created");
			} catch (FileNotFoundException ex) {
				ex.printStackTrace();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
		Log.e(TAG, "In query: entered");
		// If there are nodes in the chord ring.
		if(!selection.equals("*") && !selection.equals("@")){
			// This block of code will retrieve only one key
			Log.e(TAG, "In query: Key not in myKeys and Key not in keysReplicated.");
			String readFileName = selection;
			Log.e(TAG, "Key: "+ readFileName +" " +myKeys.contains(readFileName)+" " +keysReplicated.contains(readFileName));
			if(!(myKeys.contains(readFileName) || keysReplicated.contains(readFileName))){
				MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
				String keyHashed = getKeyId(readFileName);
				String port = "";
				if ((keyHashed.compareTo(getNodeId("5562")) > 0 && keyHashed.compareTo(getNodeId("5556")) <= 0)) {
					port = "5556";
				} else if ((keyHashed.compareTo(getNodeId("5556")) > 0 && keyHashed.compareTo(getNodeId("5554")) <= 0)) {
					port = "5554";
				} else if ((keyHashed.compareTo(getNodeId("5554")) > 0 && keyHashed.compareTo(getNodeId("5558")) <= 0)) {
					port = "5558";
				} else if ((keyHashed.compareTo(getNodeId("5558")) > 0 && keyHashed.compareTo(getNodeId("5560")) <= 0)) {
					port = "5560";
				}else {
					port = "5562";
				}
				String[] portsToQuery = new String[] {port, portsSorted.get(port)[0], portsSorted.get(port)[1]};
				for(String port1 : portsToQuery) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port1) * 2);
						//socket.setSoTimeout(1000);
						String msgToSend = "QueryKey".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(readFileName).concat("\n");
						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
						osw.write(msgToSend);
						osw.flush();
						Log.e(TAG, "In Query: Query Key request flushed" + msgToSend);

						// Receive Keys Msg from key owner
						InputStream inputStream = socket.getInputStream();
						ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
						String data = (String) objectInputStream.readObject();
						Log.e(TAG, "data : port " + data + " : " + socket.getPort());
						//if((!data.isEmpty()) && (data.split(":")[0] == readFileName)) {
						matrixCursor.newRow().add("key", data.split(":")[0]).add("value", data.split(":")[1]);
						break;
						//}
					}catch (Exception e) {
						e.printStackTrace();
						Log.e(TAG, "@@@@@@@@@@@@@@@@@@@@@@@@@@@ EXCEPTION IN QUERY KEY @@@@@@@@@@@@@@@@@@@@@@@@@@@" + e.toString());
						continue;

					}
				}
//					for(String suc : portsSorted.get(port)){
//						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//								Integer.parseInt(suc)*2);
//						msgToSend = "QueryKey".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(readFileName).concat("\n");
//						osw = new OutputStreamWriter(socket.getOutputStream());
//						osw.write(msgToSend);
//						osw.flush();
//						Log.e(TAG, "In Query: Query Key request flushed" + msgToSend);
//						// Receive Keys Msg from key owner
//						inputStream = socket.getInputStream();
//						objectInputStream = new ObjectInputStream(inputStream);
//						data =  (String) objectInputStream.readObject();
//						Log.e(TAG, "data "+ data);
//						matrixCursor.newRow().add("key", data.split(":")[0]).add("value", data.split(":")[1]);
//					}

				//return matrixCursor;
				return matrixCursor;

			}else{
				Log.e(TAG, "In Query: Key found in local.");
				List<Character> msgToSend = new ArrayList<Character>();
				try {
					FileInputStream fileInputStream;
					fileInputStream = getContext().openFileInput(readFileName);
					int char1;
					while((char1 = fileInputStream.read()) != -1){
						msgToSend.add((char)char1);
					}
				} catch (java.io.IOException e) {
					e.printStackTrace();
				}
				StringBuilder stringBuilder = new StringBuilder();
				for (Character ch : msgToSend) {
					stringBuilder.append(ch);
				}
				String msg = stringBuilder.toString();
				Log.e(TAG, msg);
				MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
				matrixCursor.newRow().add("key", readFileName).add("value", msg);
				return matrixCursor;
			}
		}
		else  if (selection.equals("@") || (selection.equals("*") && SINGLE_NODE)){
			// This block of code will retrieve all keys in local i.e. LDump
			Log.e(TAG, "In query: @");
			MatrixCursor matrixCursor = new  MatrixCursor(new String[]{"key", "value"});
			// Add own keys
			ArrayList<String> temKeys = (ArrayList<String>) myKeys.clone();
			for(String fileName: temKeys){
				String readFileName = fileName;
				List<Character> msgToSend = new ArrayList<Character>();
				try {
					FileInputStream fileInputStream;
					fileInputStream = getContext().openFileInput(readFileName);
					int char1;
					while((char1 = fileInputStream.read()) != -1){
						msgToSend.add((char)char1);
					}
				} catch (java.io.IOException e) {
					e.printStackTrace();
				}
				StringBuilder stringBuilder = new StringBuilder();
				for (Character ch : msgToSend) {
					stringBuilder.append(ch);
				}
				String msg = stringBuilder.toString();
				Log.e(TAG, String.valueOf(msgToSend));
				matrixCursor.newRow().add("key", readFileName).add("value", msg);
			}
			// Add replicated keys
			ArrayList<String> temKeys1 = (ArrayList<String>) keysReplicated.clone();
			for(String fileName: temKeys1){
				String readFileName = fileName;
				List<Character> msgToSend = new ArrayList<Character>();
				try {
					FileInputStream fileInputStream;
					fileInputStream = getContext().openFileInput(readFileName);
					int char1;
					while((char1 = fileInputStream.read()) != -1){
						msgToSend.add((char)char1);
					}
				} catch (java.io.IOException e) {
					e.printStackTrace();
				}
				StringBuilder stringBuilder = new StringBuilder();
				for (Character ch : msgToSend) {
					stringBuilder.append(ch);
				}
				String msg = stringBuilder.toString();
				Log.e(TAG, String.valueOf(msgToSend));
				matrixCursor.newRow().add("key", readFileName).add("value", msg);
			}
			return matrixCursor;
		}
		else if (selection.equals("*")){
			// This block of code will retrieve all keys in local i.e. LDump
			Log.e(TAG, "In query: *");
			MatrixCursor matrixCursor = new  MatrixCursor(new String[]{"key", "value"});
			try {
				matrixCursor = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QueryAll", myPort, myNodeId).get();
				Log.e(TAG, "MATRIX CURSOR RCV IN QUERY * AFTER QUERYING ALL: " + matrixCursor.getCount());
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			for(String fileName: myKeys){
				String readFileName = fileName;
				List<Character> msgToSend = new ArrayList<Character>();
				try {
					FileInputStream fileInputStream;
					fileInputStream = getContext().openFileInput(readFileName);
					int char1;
					while((char1 = fileInputStream.read()) != -1){
						msgToSend.add((char)char1);
					}
				} catch (java.io.IOException e) {
					e.printStackTrace();
				}
				StringBuilder stringBuilder = new StringBuilder();
				for (Character ch : msgToSend) {
					stringBuilder.append(ch);
				}
				String msg = stringBuilder.toString();
				Log.e(TAG, msg);
				matrixCursor.newRow().add("key", readFileName).add("value", msg);
			}
			return matrixCursor;
		}
//        }
		return null;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			Log.e(TAG, "Server: Server started");
			try {
				while (true) {
					Socket ss = serverSocket.accept();
					Log.e(TAG, "In Server, Client accepted");
					Log.e(TAG, ss.getRemoteSocketAddress().toString());
					InputStreamReader isr = new InputStreamReader(ss.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String msg_rcv = br.readLine();
					Log.e(TAG, "In Server, message received in server: "+ msg_rcv);
					if (msg_rcv != null) {
						String[] msgRcvd = msg_rcv.split(":");
						String taskRequested = msgRcvd[0];
						String port = String.valueOf(Integer.parseInt(msgRcvd[1])/2);

						if(taskRequested.compareTo("JOIN")==0){
							chordRingPorts.add(msgRcvd[1]);
							chordRingNodes.put(getNodeId(port),port);
							SINGLE_NODE=false;
							ArrayList<String> ports = new ArrayList<String>(chordRingNodes.values());
							int i = ports.indexOf(port);
							String pred = "";
							String succ = "";
							if(i == 0 && ports.size()>1){
								pred = ports.get(ports.size()-1);
								succ = ports.get(i+1);
							}
							else if(i == ports.size()-1){
								pred = ports.get(ports.size()-2);
								succ = ports.get(0);
							}
							else {
								pred = ports.get(i-1);
								succ = ports.get(i+1);
							}
							Log.e(TAG, "my checking for port : "+ port);
							Log.e(TAG, "my succ: "+ succ );
							Log.e(TAG, "my pred: "+ pred );
							String replyToSend = taskRequested.concat("#").concat(msgRcvd[1]).concat("#")
									.concat(succ).concat("#").concat(pred).concat("\n");
							OutputStreamWriter osw = new OutputStreamWriter(ss.getOutputStream());
							osw.write(replyToSend);
							osw.flush();

						}else if(taskRequested.compareTo("QueryAll")==0){
							// Get the port who is requesting for keys and send it the keys in your local
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							String nodeRequested = msgRcvd[2];
							String portRequested = msgRcvd[1];
							// Write Cursor object to client reply
							Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
									"@", null, null);
							String reply = "QueryAll".concat("#").concat(myNodeDetails.get("mySuccessorPort"));
							if(resultCursor.moveToFirst()) {
								do {
									String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
									String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
									reply = reply.concat("#").concat(key).concat(":").concat(value);
									Log.e(TAG, "doInBackground: " + reply);
//                                resultCursor.moveToNext();
								}while (resultCursor.moveToNext());
							}
							OutputStream outputStream = ss.getOutputStream();
							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
							objectOutputStream.writeObject(reply);

						}else if(taskRequested.compareTo("INSERT_KEY") == 0) {
							// Send Ack
							OutputStreamWriter osw = new OutputStreamWriter(ss.getOutputStream());
							osw.write("OK"+"\n");
							osw.flush();
								// Get the port who is requesting for keys and send it the keys in your local
								Log.e(TAG, "In Server, task received in server: " + taskRequested);
								// Retrieve all variables
								String key = msgRcvd[3];
								String fileContent = msgRcvd[4];
								String portRequested = msgRcvd[1];
								String nodeRequested = msgRcvd[2];
								// Call insert
								FileOutputStream fileOutputStream = getContext().openFileOutput(key, getContext().MODE_PRIVATE);
								fileOutputStream.write(fileContent.getBytes());
								fileOutputStream.close();
								Log.e(TAG, "In Insert, File created, key inserted: " + key + " in: " + myPort);
								myKeys.add(key);

								/*ContentValues contentValues = new ContentValues();
								contentValues.put("key", keyToInsert);
								contentValues.put("value", valueToInsert);
								insert(CONTENT_URI, contentValues);*/


						}else if(taskRequested.compareTo("QueryKey")==0){
							// Get the port who is requesting for keys and send it the keys in your local
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							String nodeRequested = msgRcvd[2];
							String portRequested = msgRcvd[1];
							String keyToFind = msgRcvd[3];
							// Write Cursor object to client reply
							Cursor resultCursor = query(CONTENT_URI, null,
									keyToFind, null, null);
							resultCursor.moveToFirst();
							OutputStream outputStream = ss.getOutputStream();
							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
							String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
							String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
							objectOutputStream.writeObject(key +":"+ value);

						}else if(taskRequested.compareTo("Delete")==0){
							// Send Ack
							OutputStreamWriter osw1 = new OutputStreamWriter(ss.getOutputStream());
							osw1.write("OK"+"\n");
							osw1.flush();
							// Get the port who is requesting for keys and send it the keys in your local
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							// Write Cursor object to client reply
							delete(CONTENT_URI, "@", null);
							OutputStream outputStream = ss.getOutputStream();
							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
							objectOutputStream.writeObject("Delete" +"#"+ myNodeDetails.get("mySuccessorPort"));

						}else if(taskRequested.compareTo("JOIN_UPDATE")==0){
							if(msgRcvd[3].compareTo("PRED")==0){
								myNodeDetails.put("mySuccessorPort",msgRcvd[1]);
								myNodeDetails.put("mySuccessor",getNodeId(msgRcvd[1]));
							}else if(msgRcvd[3].compareTo("SUCC")==0){
								myNodeDetails.put("myPredecessorPort",msgRcvd[1]);
								myNodeDetails.put("myPredecessor",getNodeId(msgRcvd[1]));
							}

						}else if(taskRequested.compareTo("REPLICATE_KEY") == 0){
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							// Send Ack
//							OutputStreamWriter osw1 = new OutputStreamWriter(ss.getOutputStream());
//							osw1.write("OK"+"\n");
//							osw1.flush();
							/*synchronized (INSERT_LOCK) {
								REPLICATE_KEY = Boolean.TRUE;
								// Retrieve all variables
								String keyToInsert = msgRcvd[3];
								String valueToInsert = msgRcvd[4];
								String portRequested = msgRcvd[1];
								String nodeRequested = msgRcvd[2];
								// Call insert
								ContentValues contentValues = new ContentValues();
								contentValues.put("key", keyToInsert);
								contentValues.put("value", valueToInsert);
								insert(CONTENT_URI, contentValues);
								REPLICATE_KEY = Boolean.FALSE;
							}*/
							OutputStreamWriter osw = new OutputStreamWriter(ss.getOutputStream());
							osw.write("OK"+"\n");
							osw.flush();

							String keyToInsert = msgRcvd[3];
							String valueToInsert = msgRcvd[4];
							String portRequested = msgRcvd[1];
							String nodeRequested = msgRcvd[2];
							FileOutputStream fileOutputStream;
							fileOutputStream = getContext().openFileOutput(keyToInsert, getContext().MODE_PRIVATE);
							fileOutputStream.write(valueToInsert.getBytes());
							fileOutputStream.close();
							Log.e(TAG, "In Insert, File created, replicated_key inserted: " + keyToInsert + "in: " + myPort);
							keysReplicated.add(keyToInsert);
							Log.e(TAG, "KEYS REPLICATED: " + keysReplicated.toArray().toString());

						}else if(taskRequested.compareTo("NODE_RECOVERY_PRE") == 0){
							// SEND YOUR KEYS AND YOUR PREDECESSOR KEYS TO PORT_REQUESTED
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							String nodeRequested = msgRcvd[2];
							String portRequested = msgRcvd[1];
							// Write Cursor object to client reply
							Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
									"@", null, null);
							String reply = "NODE_RECOVERY_PRE".concat("#").concat(myNodeDetails.get("mySuccessorPort"));
							if(resultCursor != null) {
								if (resultCursor.moveToFirst()) {
									do {
										String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
										String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
										reply = reply.concat("#").concat(key).concat(":").concat(value);
										Log.e(TAG, "SERVER: NODE_RECOVERY_PRE: " + reply);
									} while (resultCursor.moveToNext());
								}
							}
							reply = reply.concat("\n");
							OutputStream outputStream = ss.getOutputStream();
							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
							objectOutputStream.writeObject(reply);

						}else if(taskRequested.compareTo("NODE_RECOVERY_SUC") == 0){
							// SEND YOUR PREDECESSOR KEYS TO PORT_REQUESTED
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							String nodeRequested = msgRcvd[2];
							String portRequested = msgRcvd[1];
							String portRequestedStr = String.valueOf(Integer.parseInt(portRequested)/2);
							// Write Cursor object to client reply
							Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
									"@", null, null);
							String reply = "NODE_RECOVERY_SUC".concat("#").concat(myNodeDetails.get("mySuccessorPort"));
							if(resultCursor != null) {
								if (resultCursor.moveToFirst()) {
									do {
										String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
										String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
										reply = reply.concat("#").concat(key).concat(":").concat(value);
									} while (resultCursor.moveToNext());
								}
							}
							reply = reply.concat("\n");
							Log.e(TAG, "SERVER: NODE_RECOVERY_SUC: " + reply);
							OutputStream outputStream = ss.getOutputStream();
							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
							objectOutputStream.writeObject(reply);
							objectOutputStream.flush();

						}else if(taskRequested.compareTo("NODE_RECOVERY_ALL") == 0){
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							String nodeRequested = msgRcvd[2];
							String portRequested = msgRcvd[1];
							String portRequestedStr = String.valueOf(Integer.parseInt(portRequested)/2);
							// Write Cursor object to client reply
							Cursor resultCursor = getContext().getContentResolver().query(CONTENT_URI, null,
									"@", null, null);
							String reply = "NODE_RECOVERY_ALL".concat("#").concat(portRequestedStr);
							Log.e(TAG, "TOTAL KEYS BEING SENT: " + resultCursor.getCount());
//							if(resultCursor != null) {
							if (resultCursor.moveToFirst()) {
								do {
									String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
									String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
									reply = reply.concat("#").concat(key).concat(":").concat(value);
								} while (resultCursor.moveToNext());
							}
//							}
//							reply = reply.concat("\n");
							Log.e(TAG, "SERVER: NODE_RECOVERY_ALL: " + reply);
							OutputStream outputStream = ss.getOutputStream();
							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
							objectOutputStream.writeObject(reply);
//							objectOutputStream.flush();

						}else if(taskRequested.compareTo("DELETE_KEY")==0){
							String keyToDelete = msgRcvd[3];
							// Send Ack
							OutputStreamWriter osw1 = new OutputStreamWriter(ss.getOutputStream());
							osw1.write("OK"+"\n");
							osw1.flush();
							// Get the port who is requesting for keys and send it the keys in your local
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							// Write Cursor object to client reply
							getContext().deleteFile(keyToDelete);
							myKeys.remove(keyToDelete);
//							OutputStream outputStream = ss.getOutputStream();
//							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
//							objectOutputStream.writeObject("Delete" +"#"+ myNodeDetails.get("mySuccessorPort"));

						}else if(taskRequested.compareTo("DELETE_KEY_REPLICATION")==0){
							String keyToDelete = msgRcvd[3];
							// Send Ack
							OutputStreamWriter osw1 = new OutputStreamWriter(ss.getOutputStream());
							osw1.write("OK"+"\n");
							osw1.flush();
							// Get the port who is requesting for keys and send it the keys in your local
							Log.e(TAG, "In Server, task received in server: "+taskRequested);
							// Write Cursor object to client reply
							getContext().deleteFile(keyToDelete);
							myKeys.remove(keyToDelete);
							//delete(CONTENT_URI, keyToDelete, null);
//							OutputStream outputStream = ss.getOutputStream();
//							ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
//							objectOutputStream.writeObject("Delete" +"#"+ myNodeDetails.get("mySuccessorPort"));

						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
		@Override
		protected void onProgressUpdate(String... values) {
			//Method to update all others in the chord ring about newly joined node and ask them to update predecessors and successors
			String taskToDo = "informAboutNewlyJoinedNode";
//            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "joinUpdatePS", myPort, myNodeId);
		}
	}

	class ClientTask extends AsyncTask<String, Void, MatrixCursor>{
		@Override
		//join  joinUpdatePS
		protected MatrixCursor doInBackground(String... msgs) {
			// Initialize remotePorts array
			Log.e(TAG, "In Client, entered client task for task: "+msgs[0]+"from port: "+msgs[1]);
			String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
			// Extract request parameters
			String taskRequested = msgs[0];
			String portRequested = msgs[1];
			String nodeRequested = msgs[2];
			try {
				if(taskRequested.compareTo("QueryAll")==0) {
					MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
					String allKeys = null;
					for(String port : remotePorts){
						//if(!port.equals(myPort)){
						try {
							if(!port.equals(myPort)) {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
										Integer.parseInt(port));
								//socket.setSoTimeout(100);
								Log.e(TAG, "Connected from Client Task: taskRequested = queryAll: created socket to port: " + port);
								String msgToSend = "QueryAll".concat(":").concat(myPort).concat(":").concat(myNodeId).concat("\n");
								OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
								osw.write(msgToSend);
								osw.flush();
								Log.e(TAG, "Client: QueryAll request flushed " + msgToSend);

								// Receive Keys Msg from successor
								InputStream inputStream = socket.getInputStream();
								ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
								String keysReceived = (String) objectInputStream.readObject();
								Log.e(TAG, "Data from " + port + " " + keysReceived);
								String[] msg = keysReceived.split("#");
								String succRcv = msg[1];
								String taskRcv = msg[0];
								for (int i = 2; i < msg.length; i++) {
									matrixCursor.newRow().add("key", msg[i].split(":")[0]).add("value", msg[i].split(":")[1]);
								}
							}
						}catch (Exception e){
							Log.e(TAG, "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ EXCEPTION IN QUERY_ALL ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^	"+ e.toString());
							continue;
						}
						//}
					}
//					String[] allKeysSplit = allKeys.split("#");
//					for (int i = 0; i < allKeysSplit.length; i++) {
//						matrixCursor.newRow().add("key", allKeysSplit[i].split(":")[0]).add("value", allKeysSplit[i].split(":")[1]);
//					}
					return matrixCursor;

				}else if(taskRequested.compareTo("Delete")==0){
					for(String port: remotePorts){
						if(port.compareTo(myPort)==0){
							continue;
						}else{
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(port));
							Log.e(TAG, "Connected from Client Task: taskRequested=Delete: created socket to port: " + port);
							String msgToSend = "Delete".concat(":").concat(myPort).concat(":").concat(myNodeId).concat("\n");
							OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
							osw.write(msgToSend);
							osw.flush();
							Log.e(TAG, "Client: Delete request flushed" + msgToSend);
						}
					}
					return null;
				}else if(taskRequested.compareTo("JOIN")==0){
					//TO join
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(REMOTE_PORT0));
						Log.e(TAG, "My port: " + myPort + " sending join request to " + REMOTE_PORT0);
						String msgToSend = "JOIN".concat(":").concat(myPort).concat(":").concat(myNodeId).concat("\n");
						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
						osw.write(msgToSend);
						osw.flush();
						Log.e(TAG, "IN client: JOIN request flushed" + msgToSend);
						//Receive join request
						InputStreamReader isr = new InputStreamReader(socket.getInputStream());
						BufferedReader br = new BufferedReader(isr);
						String reply = br.readLine();
						String[] joinReply = reply.split("#");
						myNodeDetails.put("mySuccessorPort", joinReply[2]);
						myNodeDetails.put("myPredecessorPort", joinReply[3]);
						myNodeDetails.put("mySuccessor", getNodeId(joinReply[2]));
						myNodeDetails.put("myPredecessor", getNodeId(joinReply[3]));
					} catch (Exception e) {
						SINGLE_NODE = true;
						//Log.e(TAG, e.getMessage());
					}
				}
				else if(taskRequested.compareTo("JOIN_UPDATE_SC")==0) {
					// JOIN_UPDATE TO SUCCESSOR
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(myNodeDetails.get("mySuccessorPort")) * 2);
					Log.e(TAG, "My port: " + myPort + " sending join update request to " + myNodeDetails.get("mySuccessorPort"));
					String joinUpdateSucc = "JOIN_UPDATE".concat(":").concat(String.valueOf(Integer.parseInt(myPort)/2)).concat(":").concat(myNodeId).concat(":").concat("SUCC").concat("\n");
					OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
					osw.write(joinUpdateSucc);
					osw.flush();
					InputStreamReader isr = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String ack = br.readLine();
					Log.e(TAG, "IN client: JOIN_UPDATE request flushed" + joinUpdateSucc);

				}
				else if(taskRequested.compareTo("JOIN_UPDATE_PRE")==0) {
					// JOIN_UPDATE TO PREDECESSOR
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(myNodeDetails.get("myPredecessorPort"))*2);
					Log.e(TAG, "My port: " + myPort + " sending join update request to " + myNodeDetails.get("myPredecessorPort"));
					OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
					String joinUpdatePred = "JOIN_UPDATE".concat(":").concat(String.valueOf(Integer.parseInt(myPort)/2)).concat(":").concat(myNodeId).concat(":").concat("PRED").concat("\n");
					osw.write(joinUpdatePred);
					osw.flush();
					InputStreamReader isr = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String ack = br.readLine();
					Log.e(TAG, "IN client: JOIN_UPDATE request flushed" + joinUpdatePred);

				}
				else if(taskRequested.compareTo("REPLICATE_KEY")==0) {
					// REPLICATE TO SUCC1 AND SUCC2
					String key = msgs[3];
					String val = msgs[4];
					String[] portsToReplicate = portsSorted.get(portStr);
					for(String port : portsToReplicate) {
						try{
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port) * 2);
						Log.e(TAG, "Inside Client, key sent to port: " + port +" for replication");
						String msgToSend = "REPLICATE_KEY".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(key).concat(":").concat(val).concat("\n");
						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
						osw.write(msgToSend);
						osw.flush();
						InputStreamReader isr = new InputStreamReader(socket.getInputStream());
						BufferedReader br = new BufferedReader(isr);
						String ack = br.readLine();
						Log.e(TAG, "In Insert: REPLICATE_KEY request flushed" + msgToSend);
					} catch (Exception ex) {
						Log.e(TAG, "$$$$$$$$$$$$$$$$$ EXCEPTION IN INSERT_KEY IN CLIENT $$$$$$$$$$$$$$$$$" + ex.toString());
						ex.printStackTrace();
					} finally {
						continue;
					}
					}

				}
				else if(taskRequested.compareTo("NODE_FAILED_REPLICATE_KEY")==0) {
					// REPLICATE TO FAILED NODE'S SUCC1 AND SUCC2
					Log.e(TAG, "IN CLIENT: ENTERED NODE_FAILED_REPLICATE_KEY FOR KEY: "+msgs[3]);
					String key = msgs[3];
					String val = msgs[4];
					String portToSend = msgs[5];
					String[] portsToReplicate = portsSorted.get(portToSend);
					Log.e(TAG, "PORTS TO SEND REPLICATE_KEY AFTER : " + portToSend + "FAILED: " + portsToReplicate[0] + " " + portsToReplicate[1]);
					for (String port : portsToReplicate) {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port) * 2);
						Log.e(TAG, "Inside Client, key sent to port: " + port + " for replication after owner failed.");
						String msgToSend = "REPLICATE_KEY".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(key).concat(":").concat(val).concat("\n");
						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
						osw.write(msgToSend);
						osw.flush();
						InputStreamReader isr = new InputStreamReader(socket.getInputStream());
						BufferedReader br = new BufferedReader(isr);
						String ack = br.readLine();
						Log.e(TAG, "In Insert: NODE_FAILED_REPLICATE_KEY request flushed" + msgToSend);

					}

				}
				else if (taskRequested.compareTo("INSERT_KEY_REP")==0) {
					Log.e(TAG, "IN CLIENT: ENTERED REPLICATE_KEY FOR KEY: " + msgs[3]);
					String key = msgs[3];
					String val = msgs[4];
					String port = msgs[5];
					try{
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(port) * 2);
					Log.e(TAG, "Inside Client, key sent to port: " + port + " for replication after owner failed.");
					String msgToSend = "REPLICATE_KEY".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(key).concat(":").concat(val).concat("\n");
						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
					osw.write(msgToSend);
					osw.flush();
					InputStreamReader isr = new InputStreamReader(socket.getInputStream());
					BufferedReader br = new BufferedReader(isr);
					String ack = br.readLine();
					if (ack.equals("OK")) {
						Log.e(TAG, "doInBackground: SUCESS");
					}
					//Log.e(TAG, "In Insert: INSERT_REPLICATE_KEY request flushed" + msgToSend);
				} catch (Exception ex) {
					Log.e(TAG, "$$$$$$$$$$$$$$$$$ EXCEPTION IN INSERT_KEY IN CLIENT $$$$$$$$$$$$$$$$$" + ex.toString());
					ex.printStackTrace();
				}
				}else if(taskRequested.compareTo("INSERT_KEY")==0) {
//					try {
//						String key = msgs[3];
//						String val = msgs[4];
//						String port = msgs[5];
//						Log.e(TAG, "In Insert, key: " + key + "forwarding to: " + port);
//						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//								Integer.parseInt(port) * 2);
//						socket.setSoTimeout(50);
//
//						Log.e(TAG, "Inside Client, key sent to port: " + port);
//						String msgToSend = "INSERT_KEY".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(key).concat(":").concat(val).concat("\n");
//						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
//						osw.write(msgToSend);
//						osw.flush();
//						Log.e(TAG, "In Client: INSERT_KEY request flushed" + msgToSend);
//						// Read ACK
//						BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//						String insert_ack_rcv = br.readLine();
//
//					} catch (Exception e) {
//						e.printStackTrace();
//						Log.e(TAG, "$$$$$$$$$$$$$$$$$ EXCEPTION IN INSERT_KEY $$$$$$$$$$$$$$$$$" + e.toString());
					// IF ENTERED THIS BLOCK, PORT IS DOWN >> SO SEND REPLICATE REQUESTS TO BOTH ITS SUCCESSORStry

						Log.e(TAG, "IN CLIENT: ENTERED REPLICATE_KEY FOR KEY: " + msgs[3]);
						String key = msgs[3];
						String val = msgs[4];
						String portToSend = msgs[5];
						String[] portsToReplicate = portsSorted.get(portToSend);

						String coord_port = msgs[5];
						Log.e(TAG, "In Insert, key: " + key + "forwarding to: " + coord_port);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(coord_port) * 2);

						Log.e(TAG, "Inside Client, key sent to port: " + coord_port);
						String msgToSend = "INSERT_KEY".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(key).concat(":").concat(val).concat("\n");
						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
						osw.write(msgToSend);
						osw.flush();
						InputStreamReader isr = new InputStreamReader(socket.getInputStream());
						BufferedReader br = new BufferedReader(isr);
					try {
						String ack = br.readLine();
						if (ack.equals("OK")) {
							Log.e(TAG, "doInBackground: SUCESS");
						}
					} catch (Exception ex) {
						Log.e(TAG, "$$$$$$$$$$$$$$$$$ EXCEPTION IN INSERT_KEY IN CLIENT $$$$$$$$$$$$$$$$$" + ex.toString());
						ex.printStackTrace();
					}
					Log.e(TAG, "In Client: INSERT_KEY request flushed " + msgToSend);

//							Log.e(TAG, "PORTS TO SEND REPLICATE_KEY AFTER : " + portToSend + "FAILED: " + portsToReplicate[0] + " " + portsToReplicate[1]);
	/*				for (String port : portsToReplicate) {
						try {
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(port) * 2);
							Log.e(TAG, "Inside Client, key sent to port: " + port + " for replication after owner failed.");
							msgToSend = "REPLICATE_KEY".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(key).concat(":").concat(val).concat("\n");
							osw = new OutputStreamWriter(socket.getOutputStream());
							osw.write(msgToSend);
							osw.flush();
							isr = new InputStreamReader(socket.getInputStream());
							br = new BufferedReader(isr);
							String ack = br.readLine();
							if (ack.equals("OK")) {
								Log.e(TAG, "doInBackground: SUCESS");
							}
							Log.e(TAG, "In Insert: INSERT_REPLICATE_KEY request flushed" + msgToSend);
						} catch (Exception ex) {
							Log.e(TAG, "$$$$$$$$$$$$$$$$$ EXCEPTION IN INSERT_KEY IN CLIENT $$$$$$$$$$$$$$$$$" + ex.toString());
							ex.printStackTrace();
						} finally {
							continue;
						}
					}*/
				}

					//}

				else if(taskRequested.compareTo("DELETE_KEY")==0){

					String keyToDelete = msgs[3];
					String keyHashed = getKeyId(keyToDelete);
					String port = "";
					if ((keyHashed.compareTo(getNodeId("5562")) > 0 && keyHashed.compareTo(getNodeId("5556")) <= 0)) {
						port = "5556";
					} else if ((keyHashed.compareTo(getNodeId("5556")) > 0 && keyHashed.compareTo(getNodeId("5554")) <= 0)) {
						port = "5554";
					} else if ((keyHashed.compareTo(getNodeId("5554")) > 0 && keyHashed.compareTo(getNodeId("5558")) <= 0)) {
						port = "5558";
					} else if ((keyHashed.compareTo(getNodeId("5558")) > 0 && keyHashed.compareTo(getNodeId("5560")) <= 0)) {
						port = "5560";
					}else {
						port = "5562";
					}
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port) * 2);
						//socket.setSoTimeout(1000);
						String msgToSend = "DELETE_KEY".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(keyToDelete).concat("\n");
						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
						osw.write(msgToSend);
						osw.flush();
						InputStreamReader isr = new InputStreamReader(socket.getInputStream());
						BufferedReader br = new BufferedReader(isr);
						String ack = br.readLine();
						Log.e(TAG, "In CLIENT: DELETE_KEY request flushed" + msgToSend);
					}catch (Exception e) {
						e.printStackTrace();
						Log.e(TAG, "@@@@@@@@@@@@@@@@@@@@@@@@@@@ EXCEPTION IN DELETE KEY @@@@@@@@@@@@@@@@@@@@@@@@@@@" + e.toString());
					}
//					for(String suc : portsSorted.get(port)){
//						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//								Integer.parseInt(suc)*2);
//						msgToSend = "QueryKey".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(readFileName).concat("\n");
//						osw = new OutputStreamWriter(socket.getOutputStream());
//						osw.write(msgToSend);
//						osw.flush();
//						Log.e(TAG, "In Query: Query Key request flushed" + msgToSend);
//						// Receive Keys Msg from key owner
//						inputStream = socket.getInputStream();
//						objectInputStream = new ObjectInputStream(inputStream);
//						data =  (String) objectInputStream.readObject();
//						Log.e(TAG, "data "+ data);
//						matrixCursor.newRow().add("key", data.split(":")[0]).add("value", data.split(":")[1]);
//					}

					//return matrixCursor;
					return null;

				}else if(taskRequested.compareTo("DELETE_KEY_REPLICATION")==0){

					String keyToDelete = msgs[3];
					String keyHashed = getKeyId(keyToDelete);
					String port = "";
					if ((keyHashed.compareTo(getNodeId("5562")) > 0 && keyHashed.compareTo(getNodeId("5556")) <= 0)) {
						port = "5556";
					} else if ((keyHashed.compareTo(getNodeId("5556")) > 0 && keyHashed.compareTo(getNodeId("5554")) <= 0)) {
						port = "5554";
					} else if ((keyHashed.compareTo(getNodeId("5554")) > 0 && keyHashed.compareTo(getNodeId("5558")) <= 0)) {
						port = "5558";
					} else if ((keyHashed.compareTo(getNodeId("5558")) > 0 && keyHashed.compareTo(getNodeId("5560")) <= 0)) {
						port = "5560";
					}else {
						port = "5562";
					}
					String[] portsToDelete = portsSorted.get(port);
					for(String port1 : portsToDelete){
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(port1) * 2);
							String msgToSend = "DELETE_KEY_REPLICATION".concat(":").concat(myPort).concat(":").concat(myNodeId).concat(":").concat(keyToDelete).concat("\n");
							OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
							osw.write(msgToSend);
							osw.flush();
							Log.e(TAG, "In CLIENT: DELETE_KEY_REPLICATION request flushed" + msgToSend);
						}catch (Exception e) {
							e.printStackTrace();
							Log.e(TAG, "@@@@@@@@@@@@@@@@@@@@@@@@@@@ EXCEPTION IN DELETE_KEY_REPLICATION @@@@@@@@@@@@@@@@@@@@@@@@@@@" + e.toString());
						}
					}
					return null;
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	class RecoveryClientTask extends AsyncTask<String, Void, MatrixCursor>{
		@Override
		protected MatrixCursor doInBackground(String... msgs) {
			// Initialize remotePorts array
			Log.e(TAG, "In Client, entered RecoveryClientTask for task: "+msgs[0]+"from port: "+msgs[1]);
			String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
			// Extract request parameters
			String taskRequested = msgs[0];
			String portRequested = msgs[1];
			String nodeRequested = msgs[2];
			try {
//				if(taskRequested.compareTo("NODE_RECOVERY")==0) {
//					//synchronized (INSERT_LOCK) {
//						// Receive Keys From Successor. Extract Only Your Keys.
//						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//								Integer.parseInt(myNodeDetails.get("mySuccessorPort")) * 2);
//						Log.e(TAG, "Connected from NODE_RECOVERY_CLIENT: taskRequested=NODE_RECOVERY: created socket to port: " + myNodeDetails.get("mySuccessorPort"));
//						String msgToSend = "NODE_RECOVERY_SUC".concat(":").concat(myPort).concat(":").concat(myNodeId).concat("\n");
//						OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
//						osw.write(msgToSend);
//						osw.flush();
//						Log.e(TAG, "NODE_RECOVERY_CLIENT: NODE_RECOVERY_SUC request flushed" + msgToSend);
//						for(int h = 0; h<1000 ; h++){
//
//						}
//						// Receive Keys Msg from successor
//						MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
//						InputStream inputStream = socket.getInputStream();
//						ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
//						String keysReceived = (String) objectInputStream.readObject();
//						Log.e(TAG, "In NODE_RECOVERY_CLIENT: Data from " + myNodeDetails.get("mySuccessorPort") + " " + keysReceived);
//						String[] msg = keysReceived.split("#");
//						String succRcv = msg[1];
//						String taskRcv = msg[0];
//						FileOutputStream fileOutputStream;
//						for (int i = 2; i < msg.length; i++) {
//							try {
//								String port = "";
//								if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5562")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5556")) <= 0)) {
//									port = "5556";
//								} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5556")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5554")) <= 0)) {
//									port = "5554";
//								} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5554")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5558")) <= 0)) {
//									port = "5558";
//								} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5558")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5560")) <= 0)) {
//									port = "5560";
//								} else {
//									port = "5562";
//								}
//								synchronized (INSERT_LOCK) {
//									if (port.equals(portStr)) {
////										ContentValues contentValues = new ContentValues();
////										contentValues.put("key", msg[i].split(":")[0]);
////										contentValues.put("value", msg[i].split(":")[1]);
////										insert(CONTENT_URI, contentValues);
//										fileOutputStream = getContext().openFileOutput(msg[i].split(":")[0], getContext().MODE_PRIVATE);
//										fileOutputStream.write(msg[i].split(":")[1].getBytes());
//										fileOutputStream.close();
//										Log.e(TAG, "In Insert, File created, recovered_key inserted: " + msg[i].split(":")[0] + "in: " + myPort);
//										myKeys.add(msg[i].split(":")[0]);
//									}
//								}
//							} catch (Exception e) {
//								Log.e(TAG, "In NODE_RECOVERY_CLIENT, File write failed");
//							}
//						}
//						// Receive Keys From Predecessor. Extract Only Your Keys.
//						Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//								Integer.parseInt(predecessors.get(portStr)[0]));
//						Log.e(TAG, "Connected from NODE_RECOVERY_CLIENT: taskRequested=NODE_RECOVERY: created socket to port: " + myNodeDetails.get("myPredecessorPort"));
//						msgToSend = "NODE_RECOVERY_PRE".concat(":").concat(myPort).concat(":").concat(myNodeId).concat("\n");
//						OutputStreamWriter osw1 = new OutputStreamWriter(socket1.getOutputStream());
//						osw1.write(msgToSend);
//						osw1.flush();
//						Log.e(TAG, "NODE_RECOVERY_CLIENT: NODE_RECOVERY_PRE request flushed" + msgToSend);
//						// Receive Keys Msg from Predecessor
//						InputStream inputStream1 = socket1.getInputStream();
//						ObjectInputStream objectInputStream1 = new ObjectInputStream(inputStream1);
//						keysReceived = (String) objectInputStream1.readObject();
//						Log.e(TAG, "In NODE_RECOVERY_CLIENT: Data from " + myNodeDetails.get("myPredecessorPort") + " " + keysReceived);
//						msg = keysReceived.split("#");
//						succRcv = msg[1];
//						taskRcv = msg[0];
//						for (int i = 2; i < msg.length; i++) {
//							try {
//								String port = "";
//								if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5562")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5556")) <= 0)) {
//									port = "5556";
//								} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5556")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5554")) <= 0)) {
//									port = "5554";
//								} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5554")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5558")) <= 0)) {
//									port = "5558";
//								} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5558")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5560")) <= 0)) {
//									port = "5560";
//								} else {
//									port = "5562";
//								}
//								synchronized (INSERT_LOCK) {
//									if (port.compareTo(predecessors.get(portStr)[0]) == 0 || port.compareTo(predecessors.get(portStr)[1]) == 0) {
//										// Call insert
////										ContentValues contentValues = new ContentValues();
////										contentValues.put("key", msg[i].split(":")[0]);
////										contentValues.put("value", msg[i].split(":")[1]);
////										insert(CONTENT_URI, contentValues);
//										fileOutputStream = getContext().openFileOutput(msg[i].split(":")[0], getContext().MODE_PRIVATE);
//										fileOutputStream.write(msg[i].split(":")[1].getBytes());
//										fileOutputStream.close();
//										Log.e(TAG, "In NODE_RECOVERY_CLIENT, File created, recovered_replicated_key inserted: " + msg[i].split(":")[0] + "in: " + myPort);
//										keysReplicated.add(msg[i].split(":")[0]);
//									}
//								}
//							} catch (Exception e) {
//								Log.e(TAG, "In NODE_RECOVERY_CLIENT, File write failed");
//							}
//						}
//						return matrixCursor;
//					//}
//				}

				if(taskRequested.compareTo("NODE_RECOVERY")==0){
					Log.e(TAG, "ENTERED NODE_RECOVERY IN RECOVERY_CLIENT");
					for(String port : remotePorts){
						if(!(port.equals(myPort))){
							// Receive Keys. Extract Only Your Keys.
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(port));
							Log.e(TAG, "Connected from NODE_RECOVERY_CLIENT: taskRequested=NODE_RECOVERY: created socket to port: " +port);
							String msgToSend = "NODE_RECOVERY_ALL".concat(":").concat(myPort).concat(":").concat(myNodeId).concat("\n");
							OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
							osw.write(msgToSend);
							osw.flush();
							Log.e(TAG, "NODE_RECOVERY_CLIENT: NODE_RECOVERY request flushed" + msgToSend);
							// Receive Keys Msg from successor
							InputStream inputStream = socket.getInputStream();
							ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
							String keysReceived = (String) objectInputStream.readObject();
							Log.e(TAG, "In NODE_RECOVERY_CLIENT: Data from " + port + " " + keysReceived);
							String[] msg = keysReceived.split("#");
							String succRcv = msg[1];
							String taskRcv = msg[0];
							FileOutputStream fileOutputStream;
							Log.e(TAG, "MESSAGE LENGTH: "+msg.length);
							for (int i = 2; i < msg.length; i++) {
								try {
									String port1 = "";
									if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5562")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5556")) <= 0)) {
										port1 = "5556";
									} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5556")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5554")) <= 0)) {
										port1 = "5554";
									} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5554")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5558")) <= 0)) {
										port1 = "5558";
									} else if ((getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5558")) > 0 && getKeyId(msg[i].split(":")[0]).compareTo(getNodeId("5560")) <= 0)) {
										port1 = "5560";
									} else {
										port1 = "5562";
									}
										if (port1.equals(portStr)) {
											fileOutputStream = getContext().openFileOutput(msg[i].split(":")[0], getContext().MODE_PRIVATE);
											fileOutputStream.write(msg[i].split(":")[1].getBytes());
											fileOutputStream.close();
											Log.e(TAG, "In Insert, File created, recovered_key inserted: " + msg[i].split(":")[0] + "in: " + myPort);
											myKeys.add(msg[i].split(":")[0]);
										} else if(port1.equals(predecessors.get(portStr)[0]	)	||    port1.equals(predecessors.get(portStr)[1])){
											fileOutputStream = getContext().openFileOutput(msg[i].split(":")[0], getContext().MODE_PRIVATE);
											fileOutputStream.write(msg[i].split(":")[1].getBytes());
											fileOutputStream.close();
											Log.e(TAG, "In Insert, File created, recovered_replicated_key inserted: " + msg[i].split(":")[0] + "in: " + myPort);
											keysReplicated.add(msg[i].split(":")[0]);
										}

								} catch (Exception e) {
									Log.e(TAG, "In NODE_RECOVERY_CLIENT, File write failed" + e.toString());
								}
							}
						}
					}
				}

			} catch (OptionalDataException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (StreamCorruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

}
