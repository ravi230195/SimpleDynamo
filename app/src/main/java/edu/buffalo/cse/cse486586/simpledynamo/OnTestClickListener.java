package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.TextView;
/*
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
public class HelloWorld{
public static String genHash(String input) throws NoSuchAlgorithmException {


        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
     public static void main(String []args){
             String ins = "INSERT_REPLICATION";
    if (ins.equals("INSERT"))
    {
        System.out.println("TRUEEEEEEEE");
    }
         	 SortedMap<String, Integer> ring = new TreeMap<String, Integer>();
	 ArrayList<Integer> prefernceListRep = new ArrayList<Integer>();
	 ArrayList<Integer> prefernceListdup = new ArrayList<Integer>();
        ArrayList<Integer> ports = new ArrayList<Integer>(Arrays.asList(5554, 5556, 5558, 5560, 5562));
		for(int i = 0; i < ports.size(); i++)
		{
			try{
				ring.put(genHash(Integer.toString(ports.get(i))), ports.get(i));
			}
			catch (Exception e)
			{
				System.out.println("buildMembershipTables: " + e.getMessage() + e.getClass() + " for port " + ports.get(i) );
			}
		}

		ArrayList<String> rigListHash = new ArrayList<String>(ring.keySet());
		ArrayList<Integer> rigListPort = new ArrayList<Integer>(ring.values());
		int index = rigListPort.indexOf(Integer.parseInt(Integer.toString(5560)));
		System.out.println( "handleMessage: Index " + index);
		System.out.println( "handleMessage: ring port " + Arrays.toString(rigListPort.toArray()));
		System.out.println( "handleMessage: ring hash " + Arrays.toString(rigListHash.toArray()));
		int k = 2;
		int i = index;
		while(k > 0) {
			prefernceListRep.add(rigListPort.get((i+1)%5));
			i++;
			k--;
		}
		k = 2;
		i = index;
		while(k > 0) {
			if (i-1 < 0) {
				prefernceListdup.add(rigListPort.get((i-1 + 5) % 5));
			}
			else {
				prefernceListdup.add(rigListPort.get(i - 1));
			}
			i--;
			k--;
		}
		for(Integer p: prefernceListRep)
		{
			System.out.println( "buildMembershipTables: preferenceList rep |||||||||" +  p);
		}
		for(Integer p: prefernceListdup)
		{
			System.out.println( "buildMembershipTables: preferenceList dup |||||||||" +  p);
		}


		try{
		i = 0;
		String hashKey = "c5b37f0d311d28d00a0468ceb0f056e4d42723a6"; //genHash("key 49");
			for (; i< rigListHash.size(); i++) {
				String myHashTemp = genHash(Integer.toString(rigListPort.get(i)));
				index = rigListPort.indexOf(Integer.parseInt(Integer.toString(rigListPort.get(i))));
				int predecessorTemp;
				if (index-1 < 0) {
					predecessorTemp = rigListPort.get((i-1 + 5) % 5);
				}
				else {
					predecessorTemp = rigListPort.get(i - 1);
				}
				String predecessorHash = genHash(Integer.toString(predecessorTemp));
				System.out.println( String.format("insert: hashKey:[%s] predecessor:[%d] predecessorHash:[%s], myHash:[%s]", hashKey, predecessorTemp, predecessorHash, myHashTemp));
				boolean condition_1 = (myHashTemp.compareTo(predecessorHash) > 0) && (hashKey.compareTo(predecessorHash) >= 0) && (hashKey.compareTo(myHashTemp) < 0);
				boolean condition_2 = (myHashTemp.compareTo(predecessorHash) < 0) && (hashKey.compareTo(predecessorHash) >= 0 || hashKey.compareTo("0000000000000000000000000000000000000000") >= 0 && hashKey.compareTo(myHashTemp) < 0);
				String condition_1val = condition_1 ? "True" : "False";
				String condition_2val = condition_2 ? "True" : "False";
				System.out.println( String.format("insert: con1[%s] and con2[%s]", condition_1val, condition_2val));
				if (condition_1 || condition_2 || predecessorTemp == -1) {
					System.out.println("Output Value " + rigListPort.get(i));
				}
			}
		}catch (Exception e)
		{
			System.out.println( "findCordinatorPort: " + e.getClass() + e.getMessage() );
		}
     }
}
 */

public class OnTestClickListener implements OnClickListener {

    private static final String TAG = OnTestClickListener.class.getName();
    private static final int TEST_CNT = 10;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    private final ContentValues[] mContentValues;

    public OnTestClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
        mContentValues = initTestValues();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private ContentValues[] initTestValues() {
        ContentValues[] cv = new ContentValues[TEST_CNT];
        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(KEY_FIELD, "key" + Integer.toString(i));
            cv[i].put(VALUE_FIELD, "val" + Integer.toString(i));
        }

        return cv;
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            if (testInsert()) {
                publishProgress("Insert success\n");
            } else {
                publishProgress("Insert fail\n");
                return null;
            }
/*
            if (testQuery()) {
                publishProgress("Query success\n");
            } else {
                publishProgress("Query fail\n");
            }
*/
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testInsert() {
            try {
                for (int i = 0; i < TEST_CNT; i++) {
                    mContentResolver.insert(mUri, mContentValues[i]);
                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }

        private boolean testQuery() {
            try {
                for (int i = 0; i < TEST_CNT; i++) {
                    String key = (String) mContentValues[i].get(KEY_FIELD);
                    String val = (String) mContentValues[i].get(VALUE_FIELD);

                    Cursor resultCursor = mContentResolver.query(mUri, null,
                            key, null, null);
                    if (resultCursor == null) {
                        Log.e(TAG, "Result null");
                        throw new Exception();
                    }

                    int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                    int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                    if (keyIndex == -1 || valueIndex == -1) {
                        Log.e(TAG, "Wrong columns");
                        resultCursor.close();
                        throw new Exception();
                    }

                    resultCursor.moveToFirst();

                    if (!(resultCursor.isFirst() && resultCursor.isLast())) {
                        Log.e(TAG, "Wrong number of rows");
                        resultCursor.close();
                        throw new Exception();
                    }

                    String returnKey = resultCursor.getString(keyIndex);
                    String returnValue = resultCursor.getString(valueIndex);
                    if (!(returnKey.equals(key) && returnValue.equals(val))) {
                        Log.e(TAG, "(key, value) pairs don't match\n");
                        resultCursor.close();
                        throw new Exception();
                    }

                    resultCursor.close();
                }
            } catch (Exception e) {
                return false;
            }

            return true;
        }
    }
}
