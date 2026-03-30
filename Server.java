import java.io.*;
import java.net.Socket;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.*;
import javax.swing.*;

public class Server extends JFrame {

    private final JTextArea logArea = new JTextArea();
    private SSLServerSocket serverSocket;
    private final int PORT = 6667;

    private final ConcurrentHashMap<String, ClientHandler> clientsByUsername = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ClientHandler> clientsById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> userStatuses = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> activeCalls = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> userPasswords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> userIdsByUsername = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> usernameByUserId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, java.util.Set<String>> friendsByUserId =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Conversation> conversationsById = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, java.util.List<HistoryEntry>> conversationHistory =
            new ConcurrentHashMap<>();
    private final Object usersLock = new Object();
    private final Object friendsLock = new Object();
    private final Object conversationsLock = new Object();
    private final SecureRandom idRandom = new SecureRandom();
    private final File serverFilesDir = new File("server_files");
    private final File usersDb = new File("users.db");
    private final File friendsDb = new File("friends.db");
    private static final java.util.Set<String> BLOCKED_DOWNLOAD_EXTENSIONS =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "jks", "crt", "class", "db", "imi"
            ));
    private final ConcurrentHashMap<String, java.util.List<HistoryEntry>> privateHistory =
            new ConcurrentHashMap<>();
    private javax.crypto.SecretKey historyKey;
    private PrivateKey rsaPrivateKey;
    private PublicKey rsaPublicKey;

    @FunctionalInterface
    private interface StreamWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static class HistoryEntry {
        private final boolean isPrivate;
        private final String senderId;
        private final String recipientId;
        private final String encryptedBody;

        private HistoryEntry(boolean isPrivate, String senderId, String recipientId, String encryptedBody) {
            this.isPrivate = isPrivate;
            this.senderId = senderId;
            this.recipientId = recipientId;
            this.encryptedBody = encryptedBody;
        }
    }

    private static class UserRecord {
        private final String id;
        private final String username;
        private final String password;

        private UserRecord(String id, String username, String password) {
            this.id = id;
            this.username = username;
            this.password = password;
        }
    }

    private static class Conversation {
        private final String id;
        private String name;
        private final String creatorId;
        private final java.util.Set<String> members =
                java.util.Collections.synchronizedSet(new java.util.HashSet<>());

        private Conversation(String id, String name, String creatorId) {
            this.id = id;
            this.name = name;
            this.creatorId = creatorId;
        }

        private boolean addMember(String userId) {
            return members.add(userId);
        }

        private boolean isMember(String userId) {
            synchronized (members) {
                return members.contains(userId);
            }
        }

        private java.util.List<String> memberSnapshot() {
            synchronized (members) {
                return new java.util.ArrayList<>(members);
            }
        }
    }

    public Server() {

        setTitle("Chat Server (SSL)");
        setSize(600, 400);
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        logArea.setEditable(false);
        add(new JScrollPane(logArea));

        if (!serverFilesDir.exists())
            serverFilesDir.mkdirs();
        log("Server files dir: " + serverFilesDir.getAbsolutePath());

        loadUsers();
        loadFriends();
        startServer();
    }

    private void loadUsers() {
        if (!usersDb.exists()) return;
        boolean needsRewrite = false;
        java.util.List<UserRecord> records = new java.util.ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(usersDb))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\t");
                if (parts.length < 2) continue;
                String id;
                String user;
                String pass;
                if (parts.length >= 3) {
                    id = parts[0].trim();
                    user = parts[1].trim();
                    pass = parts[2].trim();
                } else {
                    user = parts[0].trim();
                    pass = parts[1].trim();
                    id = generateUserId();
                    needsRewrite = true;
                }
                if (user.isEmpty() || pass.isEmpty()) continue;
                if (userPasswords.containsKey(user)) continue;
                if (usernameByUserId.containsKey(id)) {
                    id = generateUserId();
                    needsRewrite = true;
                }
                userPasswords.put(user, pass);
                userIdsByUsername.put(user, id);
                usernameByUserId.put(id, user);
                userStatuses.putIfAbsent(id, false);
                records.add(new UserRecord(id, user, pass));
            }
        } catch (IOException e) {
            log("Users DB read error: " + e.getMessage());
        }
        if (needsRewrite && !records.isEmpty()) {
            rewriteUsersDb(records);
        }
    }

    private String registerUser(String user, String pass) {
        synchronized (usersLock) {
            if (userPasswords.containsKey(user)) {
                return null;
            }
            String id = generateUserId();
            userPasswords.put(user, pass);
            userIdsByUsername.put(user, id);
            usernameByUserId.put(id, user);
            userStatuses.put(id, false);
            try (FileWriter writer = new FileWriter(usersDb, true)) {
                writer.write(id + "\t" + user + "\t" + pass + "\n");
            } catch (IOException e) {
                userPasswords.remove(user, pass);
                userIdsByUsername.remove(user, id);
                usernameByUserId.remove(id, user);
                return null;
            }
            return id;
        }
    }

    private void rewriteUsersDb(java.util.List<UserRecord> records) {
        synchronized (usersLock) {
            try (FileWriter writer = new FileWriter(usersDb, false)) {
                for (UserRecord record : records) {
                    writer.write(record.id + "\t" + record.username + "\t" + record.password + "\n");
                }
            } catch (IOException e) {
                log("Users DB rewrite error: " + e.getMessage());
            }
        }
    }

    private void loadFriends() {
        if (!friendsDb.exists()) return;
        try (BufferedReader reader = new BufferedReader(new FileReader(friendsDb))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\t", 2);
                if (parts.length < 2) continue;
                String userId = parts[0].trim();
                String friendId = parts[1].trim();
                if (userId.isEmpty() || friendId.isEmpty()) continue;
                if (!usernameByUserId.containsKey(userId)) continue;
                if (!usernameByUserId.containsKey(friendId)) continue;
                addFriendInternal(userId, friendId, false);
            }
        } catch (IOException e) {
            log("Friends DB read error: " + e.getMessage());
        }
    }

    private boolean addFriendInternal(String userId, String friendId, boolean persist) {
        if (userId == null || friendId == null) return false;
        if (userId.equals(friendId)) return false;
        synchronized (friendsLock) {
            java.util.Set<String> set =
                    friendsByUserId.computeIfAbsent(userId,
                            k -> java.util.Collections.synchronizedSet(new java.util.HashSet<>()));
            if (!set.add(friendId)) {
                return false;
            }
            if (persist) {
                try (FileWriter writer = new FileWriter(friendsDb, true)) {
                    writer.write(userId + "\t" + friendId + "\n");
                } catch (IOException e) {
                    set.remove(friendId);
                    return false;
                }
            }
        }
        return true;
    }

    private java.util.List<String> getFriends(String userId) {
        java.util.Set<String> set = friendsByUserId.get(userId);
        if (set == null) return java.util.Collections.emptyList();
        synchronized (set) {
            return new java.util.ArrayList<>(set);
        }
    }

    private String generateConversationId() {
        String id;
        do {
            int value = 100000000 + idRandom.nextInt(900000000);
            id = "C" + value;
        } while (conversationsById.containsKey(id));
        return id;
    }

    private String sanitizeConversationName(String name, String conversationId) {
        String trimmed = name == null ? "" : name.trim();
        if (trimmed.isEmpty()) {
            return "Беседа " + conversationId;
        }
        if (trimmed.length() > 60) {
            trimmed = trimmed.substring(0, 60);
        }
        return trimmed;
    }

    private String normalizeConversationId(String id) {
        if (id == null) return null;
        if (id.startsWith("c")) {
            return "C" + id.substring(1);
        }
        return id;
    }

    private Conversation createConversation(String creatorId, String name) {
        synchronized (conversationsLock) {
            String id = generateConversationId();
            String finalName = sanitizeConversationName(name, id);
            Conversation conversation = new Conversation(id, finalName, creatorId);
            conversation.addMember(creatorId);
            conversationsById.put(id, conversation);
            return conversation;
        }
    }

    private java.util.List<Conversation> getConversationsForUser(String userId) {
        java.util.List<Conversation> list = new java.util.ArrayList<>();
        for (Conversation conversation : conversationsById.values()) {
            if (conversation.isMember(userId)) {
                list.add(conversation);
            }
        }
        return list;
    }

    private String generateUserId() {
        String id;
        do {
            int value = 100000000 + idRandom.nextInt(900000000);
            id = String.valueOf(value);
        } while (usernameByUserId.containsKey(id));
        return id;
    }

    private boolean isValidUsername(String user) {
        return user != null && user.trim().length() >= 3;
    }

    private boolean isValidPassword(String pass) {
        return pass != null && pass.length() >= 4;
    }

    private String resolveUsername(String input) {
        if (input == null) return "";
        String trimmed = input.trim();
        String byId = usernameByUserId.get(trimmed);
        return byId != null ? byId : trimmed;
    }

    private void startServer() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    historyKey = AESUtils.generateKey();
                    log("History AES key generated");
                    KeyPair rsaPair = RSAUtils.generateKeyPair();
                    rsaPrivateKey = rsaPair.getPrivate();
                    rsaPublicKey = rsaPair.getPublic();
                    log("RSA key pair generated");
                    
                    KeyStore ks = KeyStore.getInstance("JKS");
                    ks.load(new FileInputStream("serverkeystore.jks"),
                            "197826ASD".toCharArray());
                    
                    KeyManagerFactory kmf =
                            KeyManagerFactory.getInstance("SunX509");
                    kmf.init(ks, "197826ASD".toCharArray());
                    
                    SSLContext sc = SSLContext.getInstance("TLS");
                    sc.init(kmf.getKeyManagers(), null, null);
                    
                    SSLServerSocketFactory ssf = sc.getServerSocketFactory();
                    serverSocket = (SSLServerSocket) ssf.createServerSocket(PORT);
                    
                    log("SSL server started on port " + PORT);
                    
                    while (true) {
                        Socket socket = serverSocket.accept();
                        new Thread(new ClientHandler(socket)).start();
                    }
                    
                } catch (Exception e) {
                    e.printStackTrace();
                    log("Server error: " + e.getMessage());
                }
            }
        }).start();
    }

    private void log(String msg) {
        SwingUtilities.invokeLater(() -> logArea.append(msg + "\n"));
    }

    private void setUserStatus(String userId, boolean online) {
        setUserStatus(userId, online, null);
    }

    private void setUserStatus(String userId, boolean online, ClientHandler exclude) {
        if (userId == null) return;
        userStatuses.put(userId, online);
        broadcastStatusUpdate(userId, online, exclude);
    }

    private void broadcastStatusUpdate(String userId, boolean online, ClientHandler exclude) {
        String statusText = online ? "ONLINE" : "OFFLINE";
        String username = usernameByUserId.get(userId);
        for (ClientHandler c : clientsById.values()) {
            if (c == exclude) {
                continue;
            }
            try {
                c.send(stream -> {
                    stream.writeUTF("STATUS_UPDATE");
                    stream.writeUTF(userId);
                    stream.writeUTF(username == null ? "" : username);
                    stream.writeUTF(statusText);
                });
            } catch (IOException ignored) {}
        }
    }

    private void addPrivateHistory(String senderId, String recipientId, String body) throws Exception {
        String encryptedBody = AESUtils.encrypt(body, historyKey);
        String key = conversationKey(senderId, recipientId);
        java.util.List<HistoryEntry> list =
                privateHistory.computeIfAbsent(key,
                        k -> java.util.Collections.synchronizedList(new java.util.ArrayList<>()));
        list.add(new HistoryEntry(true, senderId, recipientId, encryptedBody));
        if (list.size() > 10000000) {
            list.remove(0);
        }
    }

    private void addConversationHistory(String conversationId, String senderId, String body) throws Exception {
        String encryptedBody = AESUtils.encrypt(body, historyKey);
        java.util.List<HistoryEntry> list =
                conversationHistory.computeIfAbsent(conversationId,
                        k -> java.util.Collections.synchronizedList(new java.util.ArrayList<>()));
        list.add(new HistoryEntry(false, senderId, conversationId, encryptedBody));
        if (list.size() > 10000000) {
            list.remove(0);
        }
    }

    private java.util.List<String> buildHistoryForChat(String requesterId, String targetId) {
        java.util.List<String> historyLines = new java.util.ArrayList<>();
        if (targetId == null || targetId.trim().isEmpty()) {
            return historyLines;
        }

        String key = conversationKey(requesterId, targetId);
        java.util.List<HistoryEntry> list = privateHistory.get(key);
        if (list == null) {
            return historyLines;
        }
        synchronized (list) {
            for (HistoryEntry entry : list) {
                String body;
                try {
                    body = AESUtils.decrypt(entry.encryptedBody, historyKey);
                } catch (Exception e) {
                    continue;
                }
                if (entry.senderId.equals(requesterId)) {
                    historyLines.add("[PM] you -> " + displayName(targetId) + ": " + body);
                } else {
                    historyLines.add("[PM] " + displayName(entry.senderId) + " -> you: " + body);
                }
            }
        }
        return historyLines;
    }

    private java.util.List<String> buildHistoryForConversation(String conversationId) {
        java.util.List<String> historyLines = new java.util.ArrayList<>();
        if (conversationId == null || conversationId.trim().isEmpty()) {
            return historyLines;
        }
        java.util.List<HistoryEntry> list = conversationHistory.get(conversationId);
        if (list == null) {
            return historyLines;
        }
        synchronized (list) {
            for (HistoryEntry entry : list) {
                String body;
                try {
                    body = AESUtils.decrypt(entry.encryptedBody, historyKey);
                } catch (Exception e) {
                    continue;
                }
                historyLines.add(displayName(entry.senderId) + ": " + body);
            }
        }
        return historyLines;
    }

    private String conversationKey(String idA, String idB) {
        if (idA == null || idB == null) return "";
        return idA.compareTo(idB) <= 0 ? idA + ":" + idB : idB + ":" + idA;
    }

    private String displayName(String userId) {
        String name = usernameByUserId.get(userId);
        if (name == null || name.isEmpty()) {
            return userId;
        }
        return userId + " (" + name + ")";
    }

    private void sendUsersList(ClientHandler handler) {
        if (handler == null) return;
        try {
            handler.send(stream -> {
                stream.writeUTF("USERS_LIST");
                stream.writeInt(userStatuses.size());
                for (Map.Entry<String, Boolean> entry : userStatuses.entrySet()) {
                    String id = entry.getKey();
                    String name = usernameByUserId.get(id);
                    stream.writeUTF(id);
                    stream.writeUTF(name == null ? "" : name);
                    stream.writeUTF(entry.getValue() ? "ONLINE" : "OFFLINE");
                }
            });
        } catch (IOException ignored) {}
    }

    private boolean isForbiddenDownloadFileName(String fileName) {
        if (fileName == null || fileName.trim().isEmpty()) {
            return true;
        }
        String base = new File(fileName).getName();
        if (!base.equals(fileName)) {
            return true;
        }
        String lower = base.toLowerCase(java.util.Locale.ROOT);
        int dot = lower.lastIndexOf('.');
        if (dot >= 0 && dot < lower.length() - 1) {
            String ext = lower.substring(dot + 1);
            return BLOCKED_DOWNLOAD_EXTENSIONS.contains(ext);
        }
        return false;
    }

    private boolean isInsideServerFilesDir(File file) {
        try {
            File dir = serverFilesDir.getCanonicalFile();
            File target = file.getCanonicalFile();
            String dirPath = dir.getPath() + File.separator;
            return target.getPath().startsWith(dirPath);
        } catch (IOException e) {
            return false;
        }
    }

    private void sendFriendsList(ClientHandler handler) {
        if (handler == null || handler.userId == null) return;
        java.util.List<String> friends = getFriends(handler.userId);
        try {
            handler.send(stream -> {
                stream.writeUTF("FRIENDS_LIST");
                stream.writeInt(friends.size());
                for (String id : friends) {
                    String name = usernameByUserId.get(id);
                    boolean online = userStatuses.getOrDefault(id, false);
                    stream.writeUTF(id);
                    stream.writeUTF(name == null ? "" : name);
                    stream.writeUTF(online ? "ONLINE" : "OFFLINE");
                }
            });
        } catch (IOException ignored) {}
    }

    private void sendConversationsList(ClientHandler handler) {
        if (handler == null || handler.userId == null) return;
        java.util.List<Conversation> conversations = getConversationsForUser(handler.userId);
        try {
            handler.send(stream -> {
                stream.writeUTF("CONVERSATIONS_LIST");
                stream.writeInt(conversations.size());
                for (Conversation conversation : conversations) {
                    stream.writeUTF(conversation.id);
                    stream.writeUTF(conversation.name == null ? "" : conversation.name);
                    stream.writeUTF(conversation.creatorId == null ? "" : conversation.creatorId);
                }
            });
        } catch (IOException ignored) {}
    }

    private void endCallForUser(String userId) {
        String partner = activeCalls.remove(userId);
        if (partner != null) {
            activeCalls.remove(partner);
            ClientHandler partnerHandler = clientsById.get(partner);
            if (partnerHandler != null) {
                try {
                    partnerHandler.send(stream -> {
                        stream.writeUTF("CALL_ENDED");
                        stream.writeUTF(userId);
                    });
                } catch (IOException ignored) {}
            }
        }
    }

    private class ClientHandler implements Runnable {

        private javax.crypto.SecretKey aesKey;
        private Socket socket;
        private DataInputStream in;
        private DataOutputStream out;
        private String username;
        private String userId;
        private boolean registered = false;
        private final Object outLock = new Object();

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        private void send(StreamWriter writer) throws IOException {
            if (out == null) {
                return;
            }
            synchronized (outLock) {
                writer.write(out);
                out.flush();
            }
        }

        public void run() {
            try {

                in = new DataInputStream(socket.getInputStream());
                out = new DataOutputStream(socket.getOutputStream());

                socket.setKeepAlive(true);

                String action = in.readUTF();
                if ("REGISTER".equals(action)) {
                    String user = in.readUTF();
                    String pass = in.readUTF();
                    String trimmedUser = user == null ? "" : user.trim();
                    if (!isValidUsername(trimmedUser) || !isValidPassword(pass)) {
                        send(stream -> {
                            stream.writeUTF("LOGIN_FAILED");
                            stream.writeUTF("Invalid username or password");
                        });
                        return;
                    }
                    String newId = registerUser(trimmedUser, pass);
                    if (newId == null) {
                        send(stream -> {
                            stream.writeUTF("LOGIN_FAILED");
                            stream.writeUTF("Username already exists");
                        });
                        return;
                    }
                    username = trimmedUser;
                    userId = newId;
                } else if ("LOGIN".equals(action)) {
                    String user = in.readUTF();
                    String pass = in.readUTF();
                    String trimmedUser = resolveUsername(user);
                    String stored = userPasswords.get(trimmedUser);
                    if (stored == null || !stored.equals(pass)) {
                        send(stream -> {
                            stream.writeUTF("LOGIN_FAILED");
                            stream.writeUTF("Invalid username or password");
                        });
                        return;
                    }
                    username = trimmedUser;
                    userId = userIdsByUsername.get(username);
                    if (userId == null || userId.isEmpty()) {
                        userId = generateUserId();
                        userIdsByUsername.put(username, userId);
                        usernameByUserId.put(userId, username);
                    }
                } else {
                    String resolved = resolveUsername(action);
                    username = resolved;
                    userId = userIdsByUsername.get(username);
                }

                ClientHandler existing = clientsByUsername.putIfAbsent(username, this);

                if (existing != null) {
                    send(stream -> {
                        stream.writeUTF("LOGIN_FAILED");
                        stream.writeUTF("User with this name already exists");
                    });
                    return;
                }

                registered = true;
                if (userId == null) {
                    userId = generateUserId();
                    userIdsByUsername.put(username, userId);
                    usernameByUserId.put(userId, username);
                }
                ClientHandler existingById = clientsById.putIfAbsent(userId, this);
                if (existingById != null) {
                    clientsByUsername.remove(username, this);
                    send(stream -> {
                        stream.writeUTF("LOGIN_FAILED");
                        stream.writeUTF("User with this id already connected");
                    });
                    return;
                }
                setUserStatus(userId, true, this);

                send(stream -> {
                    stream.writeUTF("LOGIN_OK");
                    stream.writeUTF(username);
                    stream.writeUTF(userId);
                });
                send(stream -> {
                    stream.writeUTF("RSA_PUBLIC_KEY");
                    stream.writeUTF(RSAUtils.publicKeyToString(rsaPublicKey));
                });
                sendUsersList(this);
                sendFriendsList(this);
                sendConversationsList(this);

                log(username + " connected");

                while (true) {
                    String type = in.readUTF();

                    if (type.equals("PING")) {
                        send(stream -> {
                    stream.writeUTF("PONG");
                    });
                        continue;
                    }

                    if (type.equals("AES_KEY")) {
                        String keyString = in.readUTF();
                        aesKey = AESUtils.stringToKey(keyString);
                    } else if (type.equals("AES_KEY_RSA")) {
                        String encryptedKey = in.readUTF();
                        String keyString = RSAUtils.decrypt(encryptedKey, rsaPrivateKey);
                        aesKey = AESUtils.stringToKey(keyString);
                    } else if (type.equals("MSG")) {
                        in.readUTF();
                        send(stream -> {
                            stream.writeUTF("PRIVATE_FAILED");
                            stream.writeUTF("Общий чат отключён");
                        });
                    } else if (type.equals("GET_SERVER_FILES")) {
                        File[] files = serverFilesDir.listFiles(file ->
                                file.isFile() && !isForbiddenDownloadFileName(file.getName()));
                        send(stream -> {
                            stream.writeUTF("SERVER_FILES_LIST");
                            if (files == null) {
                                stream.writeInt(0);
                            } else {
                                stream.writeInt(files.length);
                                for (File f : files) {
                                    stream.writeUTF(f.getName());
                                }
                            }
                        });
                    } else if (type.equals("PRIVATE")) {
                        String targetId = in.readUTF();
                        if (targetId == null || targetId.trim().isEmpty()) {
                            continue;
                        }
                        String encrypted = in.readUTF();
                        String decrypted = AESUtils.decrypt(encrypted, aesKey);

                        String targetUsername = usernameByUserId.get(targetId);
                        if (targetUsername == null) {
                            send(stream -> {
                                stream.writeUTF("PRIVATE_FAILED");
                                stream.writeUTF("Пользователь не найден");
                            });
                            continue;
                        }

                        addPrivateHistory(userId, targetId, decrypted);

                        ClientHandler target = clientsById.get(targetId);
                        if (target != null) {
                            try {
                                target.send(stream -> {
                                    stream.writeUTF("PRIVATE");
                                    stream.writeUTF(userId);
                                    stream.writeUTF(targetId);
                                    stream.writeUTF(decrypted);
                                });
                            } catch (IOException ignored) {}
                        }
                        send(stream -> {
                            stream.writeUTF("PRIVATE");
                            stream.writeUTF(userId);
                            stream.writeUTF(targetId);
                            stream.writeUTF(decrypted);
                        });
                    } else if (type.equals("GET_USERS")) {
                        sendUsersList(this);
                    } else if (type.equals("GET_FRIENDS")) {
                        sendFriendsList(this);
                    } else if (type.equals("GET_CONVERSATIONS")) {
                        sendConversationsList(this);
                    } else if (type.equals("CREATE_CONVERSATION")) {
                        String name = in.readUTF();
                        Conversation conversation = createConversation(userId, name);
                        send(stream -> {
                            stream.writeUTF("CONVERSATION_CREATED");
                            stream.writeUTF(conversation.id);
                            stream.writeUTF(conversation.name);
                            stream.writeUTF(conversation.creatorId);
                        });
                        sendConversationsList(this);
                    } else if (type.equals("INVITE_TO_CONVERSATION")) {
                        String conversationId = normalizeConversationId(in.readUTF());
                        String targetId = in.readUTF();
                        if (conversationId == null || conversationId.trim().isEmpty()
                                || targetId == null || targetId.trim().isEmpty()) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Неверные параметры");
                            });
                            continue;
                        }
                        Conversation conversation = conversationsById.get(conversationId);
                        if (conversation == null) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Беседа не найдена");
                            });
                            continue;
                        }
                        if (!conversation.isMember(userId)) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Вы не участник этой беседы");
                            });
                            continue;
                        }
                        String normalizedTargetId = targetId.trim();
                        if (normalizedTargetId.equals(userId)) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Нельзя приглашать себя");
                            });
                            continue;
                        }
                        if (!usernameByUserId.containsKey(normalizedTargetId)) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Пользователь не найден");
                            });
                            continue;
                        }
                        if (!conversation.addMember(normalizedTargetId)) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Пользователь уже в беседе");
                            });
                            continue;
                        }
                        ClientHandler targetHandler = clientsById.get(normalizedTargetId);
                        if (targetHandler != null) {
                            try {
                                targetHandler.send(s -> {
                                    s.writeUTF("CONVERSATION_ADDED");
                                    s.writeUTF(conversation.id);
                                    s.writeUTF(conversation.name);
                                    s.writeUTF(conversation.creatorId);
                                });
                            } catch (IOException ignored) {}
                        }
                    } else if (type.equals("SET_CONVERSATION_NAME")) {
                        String conversationId = normalizeConversationId(in.readUTF());
                        String newName = in.readUTF();
                        if (conversationId == null || conversationId.trim().isEmpty()) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Беседа не найдена");
                            });
                            continue;
                        }
                        Conversation conversation = conversationsById.get(conversationId.trim());
                        if (conversation == null) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Беседа не найдена");
                            });
                            continue;
                        }
                        if (!userId.equals(conversation.creatorId)) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Только создатель может менять название");
                            });
                            continue;
                        }
                        conversation.name = sanitizeConversationName(newName, conversation.id);
                        for (String memberId : conversation.memberSnapshot()) {
                            ClientHandler memberHandler = clientsById.get(memberId);
                            if (memberHandler != null) {
                                try {
                                    memberHandler.send(s -> {
                                        s.writeUTF("CONVERSATION_NAME_UPDATED");
                                        s.writeUTF(conversation.id);
                                        s.writeUTF(conversation.name);
                                    });
                                } catch (IOException ignored) {}
                            }
                        }
                    } else if (type.equals("ADD_FRIEND")) {
                        String friendId = in.readUTF();
                        if (friendId == null || friendId.trim().isEmpty()) {
                            send(stream -> {
                                stream.writeUTF("FRIEND_FAILED");
                                stream.writeUTF("Неверный ID");
                            });
                            continue;
                        }
                        final String normalizedFriendId = friendId.trim();
                        if (normalizedFriendId.equals(userId)) {
                            send(stream -> {
                                stream.writeUTF("FRIEND_FAILED");
                                stream.writeUTF("Нельзя добавить себя");
                            });
                            continue;
                        }
                        final String friendName = usernameByUserId.get(normalizedFriendId);
                        if (friendName == null) {
                            send(stream -> {
                                stream.writeUTF("FRIEND_FAILED");
                                stream.writeUTF("Пользователь не найден");
                            });
                            continue;
                        }
                        boolean added = addFriendInternal(userId, normalizedFriendId, true);
                        if (!added) {
                            send(stream -> {
                                stream.writeUTF("FRIEND_FAILED");
                                stream.writeUTF("Уже в друзьях");
                            });
                            continue;
                        }
                        send(stream -> {
                            stream.writeUTF("FRIEND_ADDED");
                            stream.writeUTF(normalizedFriendId);
                            stream.writeUTF(friendName);
                        });
                        sendFriendsList(this);
                    } else if (type.equals("GET_CHAT_HISTORY")) {
                        String targetId = in.readUTF();
                        java.util.List<String> historyLines = buildHistoryForChat(userId, targetId);
                        send(stream -> {
                            stream.writeUTF("CHAT_HISTORY");
                            stream.writeInt(historyLines.size());
                            for (String line : historyLines) {
                                stream.writeUTF(line);
                            }
                        });
                    } else if (type.equals("CONVERSATION_MSG")) {
                        String conversationId = normalizeConversationId(in.readUTF());
                        if (conversationId == null || conversationId.trim().isEmpty()) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Беседа не найдена");
                            });
                            continue;
                        }
                        Conversation conversation = conversationsById.get(conversationId.trim());
                        if (conversation == null) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Беседа не найдена");
                            });
                            continue;
                        }
                        if (!conversation.isMember(userId)) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Вы не участник этой беседы");
                            });
                            continue;
                        }
                        String encrypted = in.readUTF();
                        String decrypted = AESUtils.decrypt(encrypted, aesKey);
                        addConversationHistory(conversation.id, userId, decrypted);
                        for (String memberId : conversation.memberSnapshot()) {
                            ClientHandler memberHandler = clientsById.get(memberId);
                            if (memberHandler != null) {
                                try {
                                    memberHandler.send(s -> {
                                        s.writeUTF("CONVERSATION_MSG");
                                        s.writeUTF(conversation.id);
                                        s.writeUTF(userId);
                                        s.writeUTF(decrypted);
                                    });
                                } catch (IOException ignored) {}
                            }
                        }
                    } else if (type.equals("GET_CONVERSATION_HISTORY")) {
                        String conversationId = normalizeConversationId(in.readUTF());
                        if (conversationId == null || conversationId.trim().isEmpty()) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Беседа не найдена");
                            });
                            continue;
                        }
                        Conversation conversation = conversationsById.get(conversationId.trim());
                        if (conversation == null) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Беседа не найдена");
                            });
                            continue;
                        }
                        if (!conversation.isMember(userId)) {
                            send(stream -> {
                                stream.writeUTF("CONVERSATION_FAILED");
                                stream.writeUTF("Вы не участник этой беседы");
                            });
                            continue;
                        }
                        java.util.List<String> historyLines = buildHistoryForConversation(conversation.id);
                        send(stream -> {
                            stream.writeUTF("CONVERSATION_HISTORY");
                            stream.writeUTF(conversation.id);
                            stream.writeInt(historyLines.size());
                            for (String line : historyLines) {
                                stream.writeUTF(line);
                            }
                        });
                    } else if (type.equals("CALL_REQUEST")) {
                        String target = in.readUTF();
                        if (target == null || target.trim().isEmpty()) {
                            continue;
                        }
                        ClientHandler targetHandler = clientsById.get(target);
                        if (targetHandler == null || activeCalls.containsKey(target) || activeCalls.containsKey(userId)) {
                            send(stream -> {
                                stream.writeUTF("CALL_BUSY");
                                stream.writeUTF(target);
                            });
                            continue;
                        }
                        try {
                            targetHandler.send(stream -> {
                                stream.writeUTF("CALL_INVITE");
                                stream.writeUTF(userId);
                            });
                        } catch (IOException ignored) {}
                    } else if (type.equals("CALL_ACCEPT")) {
                        String callerName = in.readUTF();
                        if (callerName == null) {
                            continue;
                        }
                        if (activeCalls.containsKey(userId) || activeCalls.containsKey(callerName)) {
                            send(stream -> {
                                stream.writeUTF("CALL_BUSY");
                                stream.writeUTF(callerName);
                            });
                            continue;
                        }
                        ClientHandler caller = clientsById.get(callerName);
                        if (caller != null) {
                            activeCalls.put(userId, callerName);
                            activeCalls.put(callerName, userId);
                            try {
                                caller.send(stream -> {
                                    stream.writeUTF("CALL_ESTABLISHED");
                                    stream.writeUTF(userId);
                                });
                            } catch (IOException ignored) {}
                            send(stream -> {
                                stream.writeUTF("CALL_ESTABLISHED");
                                stream.writeUTF(callerName);
                            });
                        }
                    } else if (type.equals("CALL_DECLINE")) {
                        String callerName = in.readUTF();
                        ClientHandler caller = clientsById.get(callerName);
                        if (caller != null) {
                            try {
                                caller.send(stream -> {
                                    stream.writeUTF("CALL_DECLINED");
                                    stream.writeUTF(userId);
                                });
                            } catch (IOException ignored) {}
                        }
                    } else if (type.equals("CALL_BUSY")) {
                        String callerName = in.readUTF();
                        ClientHandler caller = clientsById.get(callerName);
                        if (caller != null) {
                            try {
                                caller.send(stream -> {
                                    stream.writeUTF("CALL_BUSY");
                                    stream.writeUTF(userId);
                                });
                            } catch (IOException ignored) {}
                        }
                    } else if (type.equals("CALL_END")) {
                        String peer = in.readUTF();
                        endCallForUser(userId);
                        send(stream -> {
                            stream.writeUTF("CALL_ENDED");
                            stream.writeUTF(peer);
                        });
                    } else if (type.equals("AUDIO_FRAME")) {
                        int length = in.readInt();
                        byte[] audio = new byte[length];
                        in.readFully(audio);
                        String partner = activeCalls.get(userId);
                        if (partner != null) {
                            ClientHandler partnerHandler = clientsById.get(partner);
                            if (partnerHandler != null) {
                                try {
                                    partnerHandler.send(stream -> {
                                        stream.writeUTF("AUDIO_FRAME");
                                        stream.writeInt(length);
                                        stream.write(audio);
                                    });
                                } catch (IOException ignored) {}
                            }
                        }
                    } else if (type.equals("DOWNLOAD_FILE")) {
                        String fileName = in.readUTF();
                        if (isForbiddenDownloadFileName(fileName)) {
                            send(stream -> stream.writeUTF("DOWNLOAD_FAILED"));
                            continue;
                        }
                        File file = new File(serverFilesDir, fileName);

                        if (!file.exists() || !file.isFile() || !isInsideServerFilesDir(file)) {
                            send(stream -> stream.writeUTF("DOWNLOAD_FAILED"));
                            continue;
                        }

                        byte[] data = new byte[(int) file.length()];
                        try (FileInputStream fis = new FileInputStream(file)) {
                            fis.read(data);
                        }

                        send(stream -> {
                            stream.writeUTF("DOWNLOAD_FILE");
                            stream.writeUTF(file.getName());
                            stream.writeLong(data.length);
                            stream.write(data);
                        });
                    } else if (type.equals("FILE")) {
                        String fileName = in.readUTF();
                        long size = in.readLong();

                        byte[] data = new byte[(int) size];
                        in.readFully(data);

                        File serverFile =
                                new File(serverFilesDir, username + "_" + fileName);
                        try (FileOutputStream fos = new FileOutputStream(serverFile)) {
                            fos.write(data);
                        }

                        for (ClientHandler c : clientsById.values()) {
                            try {
                                c.send(stream -> {
                                    stream.writeUTF("FILE");
                                    stream.writeUTF(username);
                                    stream.writeUTF(fileName);
                                    stream.writeLong(size);
                                    stream.write(data);
                                });
                            } catch (IOException ignored) {}
                        }

                        log("File sent: " + fileName);
                    }
                }

            } catch (Exception e) {
                if (registered) {
                    log(username + " disconnected");
                }
            } finally {
                if (registered) {
                    endCallForUser(userId);
                    if (clientsByUsername.get(username) == this) {
                        clientsByUsername.remove(username);
                    }
                    if (userId != null && clientsById.get(userId) == this) {
                        clientsById.remove(userId);
                    }
                    setUserStatus(userId, false);
                }
                try {
                    socket.close();
                } catch (Exception ignored) {}
            }
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new Server().setVisible(true));
    }
}
