package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    private static final String TAG = SimpleDynamoProvider.class.getName();
    static final int SERVER_PORT = 10000;

    String myPort;
    Uri uri = null;
    Map<String, String> avds;
    String[] avdList;
    static final int TIMEOUT = 3000;

    class Message implements Serializable {
        int version;
        String port;
        String key;
        String value;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.d(TAG, "delete selection: "+selection);

        int count = 0;

        if (selection.equals("*")) {
            for(int index=0; index < 5; index++) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(avds.get(avdList[index])));

                    socket.setSoTimeout(TIMEOUT);

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = in.readLine();
                    if (line == null) {
                        Log.e(TAG, "delete if SocketTimeoutException port: " + avds.get(avdList[index]));

                        continue;
                    }

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println("Delete");
                    printWriter.println(myPort);
                    printWriter.println(selection);
                    Log.d(TAG, "Delete send: " + selection);

                    int tempCount = Integer.parseInt(in.readLine());
                    count = count + tempCount;

                    printWriter.flush();
                    printWriter.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "delete UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "delete socket IOException port: " + avds.get(avdList[index]));
                }
            }
        }

        else if (selection.equals("@")) {
            File directory = getContext().getFilesDir();
            File[] listOfFiles = directory.listFiles();

            for (File f : listOfFiles) {
                f.delete();
                count++;
            }
        }

        else {
            String hashKey = null;

            try {
                hashKey = genHash(selection);
            } catch(NoSuchAlgorithmException e) {
                Log.e(TAG, "delete NoSuchAlgorithmException");
            }

            int replicas = 0, index = 0;
            while(replicas < 3) {
                if (replicas > 0 || (hashKey.compareTo(avdList[index]) <= 0 || (index == 0 && hashKey.compareTo(avdList[4]) > 0))) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(avds.get(avdList[index])));

                        socket.setSoTimeout(TIMEOUT);

                        InputStream inputStream = socket.getInputStream();
                        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                        String line = in.readLine();
                        if (line == null) {
                            Log.e(TAG, "delete if SocketTimeoutException port: " + avds.get(avdList[index]));

                            index = (index + 1) % 5;
                            replicas++;
                            continue;
                        }

                        PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                        printWriter.println("Delete");
                        printWriter.println(myPort);
                        printWriter.println(selection);
                        Log.d(TAG, "Delete send: " + selection);

                        int tempCount = Integer.parseInt(in.readLine());
                        count = count + tempCount;

                        printWriter.flush();
                        printWriter.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "delete UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "delete socket IOException port: " + avds.get(avdList[index]));

                        index = (index + 1) % 5;
                        replicas++;
                        continue;
                    }

                    replicas++;
                }
                index = (index + 1) % 5;
            }
        }

        return count;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Log.d(TAG, "insert: "+values.toString());
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        String hashKey = null;
        try {
            hashKey = genHash(key);
        } catch(NoSuchAlgorithmException e) {
            Log.e(TAG, "insert NoSuchAlgorithmException");
        }

        int replicas = 0, index = 0;
        String partitionPort = null;
        while(replicas < 3) {
            if(replicas > 0 || (hashKey.compareTo(avdList[index]) <= 0 || (index == 0 && hashKey.compareTo(avdList[4]) > 0))) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(avds.get(avdList[index])));

                    socket.setSoTimeout(TIMEOUT);

                    if(replicas == 0) {
                        partitionPort = avds.get(avdList[index]);
                    }

                    Log.d(TAG, "insert hashKey: "+hashKey);
                    Log.d(TAG, "insert index: "+index+", avds: "+avdList[index]+", "+avds.get(avdList[index]));

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = in.readLine();
                    if(line == null) {
                        Log.e(TAG, "insert if SocketTimeoutException port: " + avds.get(avdList[index]));

                        index = (index + 1) % 5;
                        replicas++;
                        continue;
                    }

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println("Insert");
                    printWriter.println(myPort);

                    printWriter.println(hashKey);
                    printWriter.println(key);
                    printWriter.println(value);
                    printWriter.println(partitionPort);

                    Log.d(TAG, "Insert send: " + key + ": "+ value);

                    printWriter.close();
                    socket.close();

                    replicas++;
                } catch (UnknownHostException e) {
                    Log.e(TAG, "insert UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "insert socket IOException port: "+ avds.get(avdList[index]));

                    index = (index + 1) % 5;
                    replicas++;
                    continue;
                }
            }
            index = (index + 1) % 5;
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d(TAG, myPort);

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
        uri = uriBuilder.build();

        try {
            avds = new TreeMap<String, String>();
            avdList = new String[5];
            avdList[0] = genHash("5554");
            avdList[1] = genHash("5556");
            avdList[2] = genHash("5558");
            avdList[3] = genHash("5560");
            avdList[4] = genHash("5562");

            Log.d(TAG, "onCreate avdList: "+Arrays.toString(avdList));

            avds.put(avdList[0], "11108");
            avds.put(avdList[1], "11112");
            avds.put(avdList[2], "11116");
            avds.put(avdList[3], "11120");
            avds.put(avdList[4], "11124");

            Arrays.sort(avdList);
            Log.d(TAG, "onCreate sorted avdList: "+Arrays.toString(avdList));
            Log.d(TAG, "onCreate avds: "+avds.entrySet());

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException"+e);
        }

        new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        Log.d(TAG, "query selection: "+selection);

        MatrixCursor cursor = null;

        if (selection.equals("*")) {
            Map<String, Message> hm = new HashMap<String, Message>();
            for(int index=0; index < 5; index++) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(avds.get(avdList[index])));

                    socket.setSoTimeout(TIMEOUT);

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = in.readLine();
                    if (line == null) {
                        Log.e(TAG, "query if SocketTimeoutException port: " + avds.get(avdList[index]));

                        continue;
                    }

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println("Query");
                    printWriter.println(myPort);
                    printWriter.println(selection);
                    Log.d(TAG, "Query send: " + selection);

                    ObjectInputStream inStream = new ObjectInputStream(inputStream);
                    List<List<String>> messages = (List<List<String>>) inStream.readObject();
                    Log.d(TAG, "Query messages: " + messages);

                    for(List<String> message : messages) {
                        String version = message.get(0);
                        String port = message.get(1);
                        String key = message.get(2);
                        String value = message.get(3);

                        if(!hm.containsKey(key)) {
                            Message msg = new Message();

                            msg.version = Integer.parseInt(version);
                            msg.port = port;
                            msg.key = key;
                            msg.value = value;

                            hm.put(key, msg);
                        }
                        else {
                            Message msg = hm.get(key);
                            if(Integer.parseInt(version) >= msg.version) {
                                msg.value = value;
                                msg.version = Integer.parseInt(version);

                                hm.put(key, msg);
                            }
                        }
                    }

                    printWriter.flush();
                    printWriter.close();
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "query UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "query socket IOException port: " + avds.get(avdList[index]));
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "query ClassNotFoundException");
                }

            }

            String[] columns = new String[]{"key", "value"};
            cursor = new MatrixCursor(columns);

            for(Message msg : hm.values()) {
                cursor.addRow(new Object[]{msg.key, msg.value});
                Log.d(TAG, "query key: " + msg.key + ", value: " + msg.value);
            }
        }

        else if (selection.equals("@")) {
            List<List<String>> messages = new ArrayList<List<String>>();
            File directory = getContext().getFilesDir();
            File[] listOfFiles = directory.listFiles();

            try {
                for (File f : listOfFiles) {
                    List<String> message = new ArrayList<String>();

                    BufferedReader fin = new BufferedReader(new FileReader(f));
                    String msgVersion = fin.readLine();
                    String msgPort = fin.readLine();
                    String msgKey = fin.readLine();
                    String msgValue = fin.readLine();

                    message.add(msgVersion);
                    message.add(msgPort);
                    message.add(msgKey);
                    message.add(msgValue);

                    messages.add(message);
                }
            } catch (IOException ex) {
                Log.e(TAG, "query IOException: "+ex);
            }

            Log.d(TAG, "Query messages: " + messages);
            String[] columns = new String[]{"key", "value"};
            cursor = new MatrixCursor(columns);

            for(List<String> msg : messages) {
                cursor.addRow(new Object[]{msg.get(2), msg.get(3)});
                Log.d(TAG, "query key: " + msg.get(2) + ", value: " + msg.get(3));
            }
        }

        else {
            String hashKey = null;

            try {
                hashKey = genHash(selection);
            } catch(NoSuchAlgorithmException e) {
                Log.e(TAG, "query NoSuchAlgorithmException");
            }

            int replicas = 0, index = 0, maxVersion = 1;
            String maxKey = null, maxValue = null;
            while(replicas < 3) {
                if (replicas > 0 || (hashKey.compareTo(avdList[index]) <= 0 || (index == 0 && hashKey.compareTo(avdList[4]) > 0))) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(avds.get(avdList[index])));

                        socket.setSoTimeout(TIMEOUT);

                        InputStream inputStream = socket.getInputStream();
                        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                        String line = in.readLine();
                        if (line == null) {
                            Log.e(TAG, "query if SocketTimeoutException port: " + avds.get(avdList[index]));

                            index = (index + 1) % 5;
                            replicas++;
                            continue;
                        }

                        PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                        printWriter.println("Query");
                        printWriter.println(myPort);
                        printWriter.println(selection);
                        Log.d(TAG, "Query send: " + selection);

                        ObjectInputStream inStream = new ObjectInputStream(inputStream);
                        List<String> message = (List<String>) inStream.readObject();

                        String version = message.get(0);
                        String port = message.get(1);
                        String key = message.get(2);
                        String value = message.get(3);

                        if(Integer.parseInt(version) >= maxVersion) {
                            maxKey = key;
                            maxValue = value;
                        }

                        printWriter.flush();
                        printWriter.close();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "query UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "query socket IOException port: " + avds.get(avdList[index]));

                        index = (index + 1) % 5;
                        replicas++;
                        continue;
                    } catch (ClassNotFoundException e) {
                        Log.e(TAG, "query ClassNotFoundException");
                    }

                    /* https://stackoverflow.com/questions/18290864/create-a-cursor-from-hardcoded-array-instead-of-db */
                    String[] columns = new String[]{"key", "value"};
                    cursor = new MatrixCursor(columns);
                    cursor.addRow(new Object[]{maxKey, maxValue});
                    Log.d(TAG, "query key: " + maxKey + ", value: " + maxValue);

                    replicas++;
                }
                index = (index + 1) % 5;
            }
        }

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Log.e(TAG, "ServerTask InterruptedException: "+e);
            }

            ServerSocket serverSocket = sockets[0];
            Socket socket = null;

            while(!this.isCancelled()) {
                try {
                    socket = serverSocket.accept();

                    //socket.setSoTimeout(TIMEOUT);

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println("Up");

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = in.readLine();
                    String port = in.readLine();
                    Log.d(TAG, "Server port: " + myPort + ": " + line + ": " + port);

                    if(line.equals("Insert")) {
                        String hashKey = in.readLine();
                        String key = in.readLine();
                        String value = in.readLine();
                        String partitionPort = in.readLine();
                        Log.d(TAG, "Insert server: " + key + ": " + value);

                        String filename = hashKey;

                        try {
                            File directory = getContext().getFilesDir();
                            File file = new File(directory, hashKey);
                            String version = "1";
                            if (file.exists()) {
                                BufferedReader inFile = new BufferedReader(new FileReader(file));
                                version = inFile.readLine();
                                version = String.valueOf(Integer.parseInt(version) + 1);
                                inFile.close();
                            }

                            printWriter = new PrintWriter(getContext().openFileOutput(filename, Context.MODE_PRIVATE));
                            printWriter.println(version);
                            printWriter.println(partitionPort);
                            printWriter.println(key);
                            printWriter.println(value);

                            printWriter.close();
                            Log.d(TAG, "Insert File written: " + filename + ", key: "+ key);
                        } catch (Exception e) {
                            Log.e(TAG, "Insert File write failed: " + filename + ", key: "+ key);
                        }
                    }

                    if(line.equals("Query")) {
                        String selection = in.readLine();
                        Log.d(TAG, "Query: " + selection);

                        File directory = getContext().getFilesDir();

                        if (selection.equals("*")) {
                            List<List<String>> messages = new ArrayList<List<String>>();
                            File[] listOfFiles = directory.listFiles();

                            for(File f : listOfFiles) {
                                List<String> message = new ArrayList<String>();

                                BufferedReader fin = new BufferedReader(new FileReader(f));
                                String msgVersion = fin.readLine();
                                String msgPort = fin.readLine();
                                String msgKey = fin.readLine();
                                String msgValue = fin.readLine();

                                message.add(msgVersion);
                                message.add(msgPort);
                                message.add(msgKey);
                                message.add(msgValue);

                                messages.add(message);
                            }

                            Log.d(TAG, "Query server messages: " + messages);
                            ObjectOutputStream outputObjectStream = new ObjectOutputStream(socket.getOutputStream());
                            outputObjectStream.writeObject(messages);

                            outputObjectStream.close();
                        }

                        else {
                            String hashKey = null;
                            List<String> message = new ArrayList<String>();

                            try {
                                hashKey = genHash(selection);
                            } catch(NoSuchAlgorithmException e) {
                                Log.e(TAG, "Query NoSuchAlgorithmException: "+e);
                            }

                            File file = new File(directory, hashKey);

                            BufferedReader fin = new BufferedReader(new FileReader(file));
                            message.add(fin.readLine());
                            message.add(fin.readLine());
                            message.add(fin.readLine());
                            message.add(fin.readLine());

                            Log.d(TAG, "Query server message: " + message);
                            ObjectOutputStream outputObjectStream = new ObjectOutputStream(socket.getOutputStream());
                            outputObjectStream.writeObject(message);

                            outputObjectStream.close();
                        }
                    }

                    if(line.equals("Delete")) {
                        String selection = in.readLine();
                        Log.d(TAG, "Delete: " + selection);

                        int count = 0;
                        File directory = getContext().getFilesDir();

                        if (selection.equals("*")) {
                            File[] listOfFiles = directory.listFiles();

                            for(File f : listOfFiles) {
                                f.delete();
                                count++;
                            }
                        }

                        else {
                            String hashKey = null;

                            try {
                                hashKey = genHash(selection);
                            } catch(NoSuchAlgorithmException e) {
                                Log.e(TAG, "delete NoSuchAlgorithmException");
                            }

                            File file = new File(directory, hashKey);

                            if (file.exists()) {
                                file.delete();
                                count++;
                            }
                        }
                        printWriter.println(count);
                    }

                    if(line.equals("Recover1")) {
                        File directory = getContext().getFilesDir();
                        List<List<String>> messages = new ArrayList<List<String>>();
                        File[] listOfFiles = directory.listFiles();

                        for(File f : listOfFiles) {
                            List<String> message = new ArrayList<String>();

                            BufferedReader fin = new BufferedReader(new FileReader(f));
                            String msgVersion = fin.readLine();
                            String msgPort = fin.readLine();
                            String msgKey = fin.readLine();
                            String msgValue = fin.readLine();

                            if(msgPort.equals(port)) {
                                message.add(msgVersion);
                                message.add(msgPort);
                                message.add(msgKey);
                                message.add(msgValue);

                                messages.add(message);
                            }
                        }

                        Log.d(TAG, "Recover1 server messages: " + messages);
                        ObjectOutputStream outputObjectStream = new ObjectOutputStream(socket.getOutputStream());
                        outputObjectStream.writeObject(messages);

                        outputObjectStream.close();
                    }

                    if(line.equals("Recover2")) {
                        File directory = getContext().getFilesDir();
                        List<List<String>> messages = new ArrayList<List<String>>();
                        File[] listOfFiles = directory.listFiles();

                        for(File f : listOfFiles) {
                            List<String> message = new ArrayList<String>();

                            BufferedReader fin = new BufferedReader(new FileReader(f));
                            String msgVersion = fin.readLine();
                            String msgPort = fin.readLine();
                            String msgKey = fin.readLine();
                            String msgValue = fin.readLine();

                            if(msgPort.equals(myPort)) {
                                message.add(msgVersion);
                                message.add(msgPort);
                                message.add(msgKey);
                                message.add(msgValue);

                                messages.add(message);
                            }
                        }

                        Log.d(TAG, "Recover2 server messages: " + messages);
                        ObjectOutputStream outputObjectStream = new ObjectOutputStream(socket.getOutputStream());
                        outputObjectStream.writeObject(messages);

                        outputObjectStream.close();
                    }

                    printWriter.close();
                    in.close();
                } catch (SocketTimeoutException ex) {
                    Log.e(TAG, "ServerTask socket SocketTimeoutException: "+ex);
                } catch (StreamCorruptedException ex) {
                    Log.e(TAG, "ServerTask socket StreamCorruptedException: "+ex);
                } catch (EOFException ex) {
                    Log.e(TAG, "ServerTask socket EOFException: "+ex);
                } catch (UnknownHostException ex) {
                    Log.e(TAG, "ServerTask UnknownHostException: "+ex);
                } catch (IOException ex) {
                    Log.e(TAG, "ServerTask IOException: "+ex);
                } catch (Exception ex) {
                    Log.e(TAG, "ServerTask Exception: "+ex);
                } finally {
                    try {
                        if (socket != null) {
                            socket.close();
                        }
                    } catch (IOException ex) {
                        Log.e(TAG, "ServerTask socket close IOException: "+ex.getCause());
                    }
                }
            }

            return null;
        }
    }

    private class RecoveryTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            File directory = getContext().getFilesDir();
            File[] listOfFiles = directory.listFiles();

            for (File f : listOfFiles) {
                f.delete();
            }

            String hashPort = null;
            try {
                hashPort = genHash(String.valueOf(Integer.parseInt(myPort) / 2));
            } catch(NoSuchAlgorithmException e) {
                Log.e(TAG, "RecoveryTask NoSuchAlgorithmException");
            }

            Map<String, Message> hm = new HashMap<String, Message>();
            int index = 0;
            for(int i=0; i < 5; i++) {
                if(hashPort.equals(avdList[i])) {
                    index = i;
                    break;
                }
            }

            for(int i=0; i < 2; i++) {
                index = (index + 1) % 5;

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(avds.get(avdList[index])));

                    socket.setSoTimeout(TIMEOUT);

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = in.readLine();
                    if (line == null) {
                        Log.e(TAG, "RecoveryTask1 if SocketTimeoutException port: " + avds.get(avdList[index]));

                        continue;
                    }

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println("Recover1");
                    printWriter.println(myPort);
                    Log.d(TAG, "RecoveryTask1 send: "+myPort);

                    ObjectInputStream inStream = new ObjectInputStream(inputStream);
                    List<List<String>> messages = (List<List<String>>) inStream.readObject();
                    Log.d(TAG, "RecoveryTask1 messages: " + messages);

                    for(List<String> message : messages) {
                        String version = message.get(0);
                        String port = message.get(1);
                        String key = message.get(2);
                        String value = message.get(3);

                        if(!hm.containsKey(key)) {
                            Message msg = new Message();

                            msg.version = Integer.parseInt(version);
                            msg.port = port;
                            msg.key = key;
                            msg.value = value;

                            hm.put(key, msg);
                        }
                        else {
                            Message msg = hm.get(key);
                            if(Integer.parseInt(version) >= msg.version) {
                                msg.value = value;
                                msg.version = Integer.parseInt(version);

                                hm.put(key, msg);
                            }
                        }
                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "RecoveryTask1 UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "RecoveryTask1 socket IOException port: " + avds.get(avdList[index]));
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "RecoveryTask1 ClassNotFoundException");
                }
            }

            index = (index + 8) % 5;

            for(int i=0; i < 2; i++) {
                index = (index + 4) % 5;

                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(avds.get(avdList[index])));

                    socket.setSoTimeout(TIMEOUT);

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
                    String line = in.readLine();
                    if (line == null) {
                        Log.e(TAG, "RecoveryTask2 if SocketTimeoutException port: " + avds.get(avdList[index]));

                        continue;
                    }

                    PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
                    printWriter.println("Recover2");
                    printWriter.println(myPort);
                    Log.d(TAG, "RecoveryTask2 send: "+myPort);

                    ObjectInputStream inStream = new ObjectInputStream(inputStream);
                    List<List<String>> messages = (List<List<String>>) inStream.readObject();
                    Log.d(TAG, "RecoveryTask2 messages: " + messages);

                    for(List<String> message : messages) {
                        String version = message.get(0);
                        String port = message.get(1);
                        String key = message.get(2);
                        String value = message.get(3);

                        if(!hm.containsKey(key)) {
                            Message msg = new Message();

                            msg.version = Integer.parseInt(version);
                            msg.port = port;
                            msg.key = key;
                            msg.value = value;

                            hm.put(key, msg);
                        }
                        else {
                            Message msg = hm.get(key);
                            if(Integer.parseInt(version) >= msg.version) {
                                msg.value = value;
                                msg.version = Integer.parseInt(version);
                                msg.port = port;

                                hm.put(key, msg);
                            }
                        }
                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "RecoveryTask2 UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "RecoveryTask2 socket IOException port: " + avds.get(avdList[index]));
                } catch (ClassNotFoundException e) {
                    Log.e(TAG, "RecoveryTask2 ClassNotFoundException");
                }
            }

            Log.d(TAG, "RecoveryTask hm: " + hm.entrySet());
            for(Message msg : hm.values()) {
                String hashKey = null;

                try {
                    hashKey = genHash(msg.key);
                } catch (NoSuchAlgorithmException e) {
                    Log.e(TAG, "RecoveryTask NoSuchAlgorithmException");
                }

                String filename = hashKey;

                try {
                    PrintWriter printWriter = new PrintWriter(getContext().openFileOutput(filename, Context.MODE_PRIVATE));
                    printWriter.println(msg.version);
                    printWriter.println(msg.port);
                    printWriter.println(msg.key);
                    printWriter.println(msg.value);

                    printWriter.close();
                    Log.d(TAG, "RecoveryTask: File written: " + filename + ", key: "+ msg.key);
                } catch (Exception e) {
                    Log.e(TAG, "RecoveryTask: File write failed: " + filename + ", key: "+ msg.key);
                }
            }

            return null;
        }
    }
}
