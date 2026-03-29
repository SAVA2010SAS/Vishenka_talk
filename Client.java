import java.awt.*;
import java.io.*;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.*;
import javax.sound.sampled.*;
import javax.swing.*;
import javax.swing.border.EmptyBorder;

public class Client extends JFrame {

    private JTextArea chatArea = new JTextArea();
    private JTextField inputField = new JTextField();
    private JTextField loginField = new JTextField(12);
    private JPasswordField passwordField = new JPasswordField(12);
    private JTextField userIdField = new JTextField(12);
    private JTextField targetIdField = new JTextField(12);
    private JButton openChatBtn = new JButton("Открыть чат");
    private JButton createConversationBtn = new JButton("Создать беседу");
    private JButton inviteToConversationBtn = new JButton("Пригласить");
    private JButton renameConversationBtn = new JButton("Переименовать");
    private JButton addFriendBtn = new JButton("Добавить в друзья");
    private JButton pinChatBtn = new JButton("Закрепить");

    private JButton loginBtn = new JButton("Войти / Регистрация");
    private JButton sendBtn = new JButton("Отправить");
    private JButton fileBtn = new JButton("Отправить файл");
    private JButton downloadBtn = new JButton("Скачать");
    private JButton callBtn = new JButton("Позвонить");

    private DefaultListModel<String> usersListModel = new DefaultListModel<>();
    private JList<String> usersList = new JList<>(usersListModel);

    private javax.crypto.SecretKey aesKey;
    private SSLSocket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private String username;
    private java.security.PublicKey serverPublicKey;

    private final Map<String, String> userStatuses = new ConcurrentHashMap<>();
    private final Map<String, String> userNamesById = new ConcurrentHashMap<>();
    private final Map<String, String> conversationNamesById = new ConcurrentHashMap<>();
    private final Map<String, String> conversationCreatorsById = new ConcurrentHashMap<>();
    private final java.util.Set<String> friends =
            java.util.Collections.synchronizedSet(new java.util.HashSet<>());
    private final java.util.Set<String> pinnedChats =
            java.util.Collections.synchronizedSet(new java.util.HashSet<>());
    private final File downloadsDir =
            new File(System.getProperty("user.home") + File.separator + "Downloads");
    private final File pinnedDb = new File("pinned_chats.db");
    private JDialog callDialog;
    private String activeCallPeer;
    private volatile boolean inCall;
    private volatile boolean capturing;
    private TargetDataLine microphone;
    private SourceDataLine speaker;
    private Thread captureThread;
    private final AudioFormat audioFormat = new AudioFormat(16000f, 16, 1, true, false);
    private String selfUserId;
    private String currentChatId;

    // Sound clips
    private Clip messageSentSound;
    private Clip messageReceivedSound;
    private Clip incomingCallSound;
    private boolean soundsLoaded = false;

    public Client() {

        setTitle("ВишенкаЧат");
        setSize(760, 520);
        setMinimumSize(new Dimension(680, 460));
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        Color windowBg = new Color(233, 236, 240);
        Color panelBg = new Color(245, 247, 250);
        Color cardBg = Color.WHITE;
        Color accent = new Color(36, 121, 234);
        Color sectionText = new Color(70, 70, 70);

        Font uiFont = new Font("Segoe UI", Font.PLAIN, 12);
        Font headerFont = new Font("Segoe UI", Font.BOLD, 12);

        JPanel root = new JPanel(new BorderLayout(10, 10));
        root.setBorder(new EmptyBorder(10, 10, 10, 10));
        root.setBackground(windowBg);
        setContentPane(root);

        chatArea.setEditable(false);
        chatArea.setLineWrap(true);
        chatArea.setWrapStyleWord(true);
        chatArea.setBorder(new EmptyBorder(8, 8, 8, 8));
        chatArea.setBackground(cardBg);
        chatArea.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        setControlsEnabled(false);
        downloadBtn.setEnabled(false);
        loginField.setEditable(false);
        passwordField.setEditable(false);
        userIdField.setEditable(false);
        inputField.setFont(uiFont);
        loginField.setFont(uiFont);
        passwordField.setFont(uiFont);
        userIdField.setFont(uiFont);
        targetIdField.setFont(uiFont);
        usersList.setFont(uiFont);

        JPanel left = new JPanel();
        left.setLayout(new BoxLayout(left, BoxLayout.Y_AXIS));
        left.setBackground(panelBg);
        left.setBorder(new EmptyBorder(10, 10, 10, 10));
        left.setPreferredSize(new Dimension(240, 0));

        JLabel accountLabel = new JLabel("Аккаунт");
        accountLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        accountLabel.setFont(headerFont);
        accountLabel.setForeground(sectionText);
        left.add(accountLabel);
        left.add(Box.createVerticalStrut(4));

        JLabel emailLabel = new JLabel("Логин / ID");
        emailLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        emailLabel.setFont(uiFont);
        emailLabel.setForeground(sectionText);
        left.add(emailLabel);
        left.add(Box.createVerticalStrut(4));

        JPanel emailRow = new JPanel(new BorderLayout(6, 6));
        emailRow.setBackground(panelBg);
        emailRow.add(loginField, BorderLayout.CENTER);
        emailRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(emailRow);
        left.add(Box.createVerticalStrut(8));

        JLabel nameLabel = new JLabel("Пароль");
        nameLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        nameLabel.setFont(uiFont);
        nameLabel.setForeground(sectionText);
        left.add(nameLabel);
        left.add(Box.createVerticalStrut(4));

        JPanel loginRow = new JPanel(new BorderLayout(6, 6));
        loginRow.setBackground(panelBg);
        loginRow.add(passwordField, BorderLayout.CENTER);
        loginRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(loginRow);
        left.add(Box.createVerticalStrut(6));

        JLabel idLabel = new JLabel("Ваш ID");
        idLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        idLabel.setFont(uiFont);
        idLabel.setForeground(sectionText);
        left.add(idLabel);
        left.add(Box.createVerticalStrut(4));

        JPanel idRow = new JPanel(new BorderLayout(6, 6));
        idRow.setBackground(panelBg);
        idRow.add(userIdField, BorderLayout.CENTER);
        idRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(idRow);
        left.add(Box.createVerticalStrut(8));

        JPanel authRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        authRow.setBackground(panelBg);
        authRow.add(loginBtn);
        authRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(authRow);
        left.add(Box.createVerticalStrut(8));
        JSeparator accountSeparator = new JSeparator();
        accountSeparator.setMaximumSize(new Dimension(Integer.MAX_VALUE, 1));
        left.add(accountSeparator);
        left.add(Box.createVerticalStrut(8));

        JLabel targetLabel = new JLabel("ID собеседника");
        targetLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        targetLabel.setFont(uiFont);
        targetLabel.setForeground(sectionText);
        left.add(targetLabel);
        left.add(Box.createVerticalStrut(4));

        JPanel targetRow = new JPanel(new BorderLayout(6, 6));
        targetRow.setBackground(panelBg);
        targetRow.add(targetIdField, BorderLayout.CENTER);
        targetRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(targetRow);
        left.add(Box.createVerticalStrut(6));

        JPanel openChatRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        openChatRow.setBackground(panelBg);
        openChatRow.add(openChatBtn);
        openChatRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(openChatRow);
        left.add(Box.createVerticalStrut(6));

        JLabel conversationsLabel = new JLabel("Беседы");
        conversationsLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        conversationsLabel.setFont(headerFont);
        conversationsLabel.setForeground(sectionText);
        left.add(conversationsLabel);
        left.add(Box.createVerticalStrut(4));

        JPanel conversationCreateRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        conversationCreateRow.setBackground(panelBg);
        conversationCreateRow.add(createConversationBtn);
        conversationCreateRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(conversationCreateRow);
        left.add(Box.createVerticalStrut(4));

        JPanel conversationInviteRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        conversationInviteRow.setBackground(panelBg);
        conversationInviteRow.add(inviteToConversationBtn);
        conversationInviteRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(conversationInviteRow);
        left.add(Box.createVerticalStrut(4));

        JPanel conversationRenameRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        conversationRenameRow.setBackground(panelBg);
        conversationRenameRow.add(renameConversationBtn);
        conversationRenameRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(conversationRenameRow);
        left.add(Box.createVerticalStrut(6));
        JSeparator conversationSeparator = new JSeparator();
        conversationSeparator.setMaximumSize(new Dimension(Integer.MAX_VALUE, 1));
        left.add(conversationSeparator);
        left.add(Box.createVerticalStrut(8));

        JPanel friendRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        friendRow.setBackground(panelBg);
        friendRow.add(addFriendBtn);
        friendRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(friendRow);
        left.add(Box.createVerticalStrut(6));

        JPanel pinRow = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        pinRow.setBackground(panelBg);
        pinRow.add(pinChatBtn);
        pinRow.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(pinRow);
        left.add(Box.createVerticalStrut(6));
        JSeparator actionsSeparator = new JSeparator();
        actionsSeparator.setMaximumSize(new Dimension(Integer.MAX_VALUE, 1));
        left.add(actionsSeparator);
        left.add(Box.createVerticalStrut(8));

        JPanel actions = new JPanel(new GridLayout(0, 1, 6, 6));
        actions.setBackground(panelBg);
        actions.add(callBtn);
        actions.add(downloadBtn);
        actions.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(actions);
        left.add(Box.createVerticalStrut(12));

        JLabel usersLabel = new JLabel("Чаты");
        usersLabel.setAlignmentX(Component.LEFT_ALIGNMENT);
        usersLabel.setFont(headerFont);
        usersLabel.setForeground(sectionText);
        left.add(usersLabel);
        left.add(Box.createVerticalStrut(6));

        usersList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        usersList.setBackground(cardBg);
        usersList.setBorder(new EmptyBorder(4, 4, 4, 4));
        usersList.setFixedCellHeight(22);
        usersList.setCellRenderer((list, value, index, isSelected, cellHasFocus) -> {
            DefaultListCellRenderer renderer = new DefaultListCellRenderer();
            JLabel label = (JLabel) renderer.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            if (isConversationId(value)) {
                String name = conversationNamesById.getOrDefault(value, value);
                String base = name + " (" + value + ")";
                StringBuilder text = new StringBuilder();
                if (pinnedChats.contains(value)) {
                    text.append("[PIN] ");
                }
                text.append(base).append("  •  беседа");
                label.setText(text.toString());
                if (!isSelected) {
                    label.setForeground(new Color(60, 60, 60));
                }
            } else {
                String status = userStatuses.getOrDefault(value, "OFFLINE");
                String name = userNamesById.getOrDefault(value, "");
                String base = value;
                if (!name.isEmpty()) {
                    base = value + "  •  " + name;
                }
                StringBuilder text = new StringBuilder();
                if (pinnedChats.contains(value)) {
                    text.append("[PIN] ");
                }
                text.append(base).append("  •  ").append(status.toLowerCase());
                if (friends.contains(value)) {
                    text.append("  •  friend");
                }
                label.setText(text.toString());
                if (!isSelected) {
                    label.setForeground("ONLINE".equals(status) ? new Color(32, 130, 70) : new Color(120, 120, 120));
                }
            }
            return label;
        });

        usersList.addListSelectionListener(e -> {
            if (e.getValueIsAdjusting()) return;
            String selected = usersList.getSelectedValue();
            if (selected == null) return;
            if (isConversationId(selected)) {
                targetIdField.setText("");
                setActiveChat(selected);
            } else {
                targetIdField.setText(selected);
                setActiveChat(selected);
            }
        });

        JScrollPane usersScroll = new JScrollPane(usersList);
        usersScroll.setBorder(new EmptyBorder(0, 0, 0, 0));
        usersScroll.setAlignmentX(Component.LEFT_ALIGNMENT);
        left.add(usersScroll);

        JPanel bottom = new JPanel(new BorderLayout(8, 6));
        bottom.setBackground(windowBg);

        bottom.add(inputField, BorderLayout.CENTER);

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        buttons.setBackground(windowBg);
        buttons.add(sendBtn);
        buttons.add(fileBtn);

        bottom.add(buttons, BorderLayout.EAST);

        JScrollPane chatScroll = new JScrollPane(chatArea);
        chatScroll.setBorder(new EmptyBorder(0, 0, 0, 0));

        root.add(left, BorderLayout.WEST);
        root.add(chatScroll, BorderLayout.CENTER);
        root.add(bottom, BorderLayout.SOUTH);

        Insets buttonInsets = new Insets(4, 12, 4, 12);
        loginBtn.setMargin(buttonInsets);
        sendBtn.setMargin(buttonInsets);
        fileBtn.setMargin(buttonInsets);
        downloadBtn.setMargin(buttonInsets);
        callBtn.setMargin(buttonInsets);
        openChatBtn.setMargin(buttonInsets);
        createConversationBtn.setMargin(buttonInsets);
        inviteToConversationBtn.setMargin(buttonInsets);
        renameConversationBtn.setMargin(buttonInsets);
        addFriendBtn.setMargin(buttonInsets);
        pinChatBtn.setMargin(buttonInsets);
        loginBtn.setFont(uiFont);
        sendBtn.setFont(uiFont);
        fileBtn.setFont(uiFont);
        downloadBtn.setFont(uiFont);
        callBtn.setFont(uiFont);
        openChatBtn.setFont(uiFont);
        createConversationBtn.setFont(uiFont);
        inviteToConversationBtn.setFont(uiFont);
        renameConversationBtn.setFont(uiFont);
        addFriendBtn.setFont(uiFont);
        pinChatBtn.setFont(uiFont);
        sendBtn.setBackground(accent);
        sendBtn.setForeground(Color.WHITE);
        sendBtn.setOpaque(true);

        loginBtn.addActionListener(e -> showAuthDialog());
        sendBtn.addActionListener(e -> sendMessage());
        inputField.addActionListener(e -> sendMessage());
        fileBtn.addActionListener(e -> sendFile());
        downloadBtn.addActionListener(e -> requestServerFiles());
        callBtn.addActionListener(e -> showCallSelector());
        openChatBtn.addActionListener(e -> openChatById());
        targetIdField.addActionListener(e -> openChatById());
        addFriendBtn.addActionListener(e -> addFriendById());
        pinChatBtn.addActionListener(e -> togglePinnedChat());
        createConversationBtn.addActionListener(e -> createConversation());
        inviteToConversationBtn.addActionListener(e -> inviteToConversation());
        renameConversationBtn.addActionListener(e -> renameConversation());

        // Load sounds
        loadSounds();

        SwingUtilities.invokeLater(this::showAuthDialog);
    }

    private void loadSounds() {
        try {
            // Create simple beep sounds programmatically
            messageSentSound = createBeepSound(800, 100);
            messageReceivedSound = createBeepSound(600, 150);
            incomingCallSound = createRingtoneSound();
            soundsLoaded = true;
        } catch (LineUnavailableException e) {
            System.err.println("Could not load sounds: " + e.getMessage());
            soundsLoaded = false;
        }
    }

    private Clip createBeepSound(float frequency, int durationMs) throws LineUnavailableException {
        AudioFormat format = new AudioFormat(44100, 16, 1, true, false);
        byte[] buffer = new byte[(int)(format.getSampleRate() * durationMs / 1000) * 2];
        
        for (int i = 0; i < buffer.length / 2; i++) {
            double angle = 2.0 * Math.PI * i / (format.getSampleRate() / frequency);
            short sample = (short) (Math.sin(angle) * 16384);
            buffer[i * 2] = (byte) (sample & 0xFF);
            buffer[i * 2 + 1] = (byte) ((sample >> 8) & 0xFF);
        }
        
        Clip clip = AudioSystem.getClip();
        clip.open(format, buffer, 0, buffer.length);
        return clip;
    }

    private Clip createRingtoneSound() throws LineUnavailableException {
        AudioFormat format = new AudioFormat(44100, 16, 1, true, false);
        int durationMs = 2000;
        int patternMs = 500;
        byte[] buffer = new byte[(int)(format.getSampleRate() * durationMs / 1000) * 2];
        
        for (int i = 0; i < buffer.length / 2; i++) {
            double timeMs = i / (format.getSampleRate() / 1000.0);
            double frequency;
            if (timeMs % (patternMs * 2) < patternMs) {
                frequency = 800;
            } else {
                frequency = 600;
            }
            double angle = 2.0 * Math.PI * i / (format.getSampleRate() / frequency);
            short sample = (short) (Math.sin(angle) * 16384);
            buffer[i * 2] = (byte) (sample & 0xFF);
            buffer[i * 2 + 1] = (byte) ((sample >> 8) & 0xFF);
        }
        
        Clip clip = AudioSystem.getClip();
        clip.open(format, buffer, 0, buffer.length);
        return clip;
    }

    private void playSound(Clip clip) {
        if (!soundsLoaded || clip == null) return;
        try {
            if (clip.isRunning()) {
                clip.stop();
            }
            clip.setFramePosition(0);
            clip.start();
        } catch (Exception e) {
            // Silently fail if sound can't play
        }
    }

    private void playMessageSentSound() {
        playSound(messageSentSound);
    }

    private void playMessageReceivedSound() {
        playSound(messageReceivedSound);
    }

    private void playIncomingCallSound() {
        playSound(incomingCallSound);
    }

    private void setControlsEnabled(boolean enabled) {
        sendBtn.setEnabled(enabled);
        fileBtn.setEnabled(enabled);
        callBtn.setEnabled(enabled);
        downloadBtn.setEnabled(enabled);
        openChatBtn.setEnabled(enabled);
        targetIdField.setEnabled(enabled);
        usersList.setEnabled(enabled);
        addFriendBtn.setEnabled(enabled);
        createConversationBtn.setEnabled(enabled);
        pinChatBtn.setEnabled(enabled && currentChatId != null);
        updateConversationControls();
    }

    private void updateStatusLabel(boolean online) {
        // Status label removed from UI; keep method for compatibility.
    }

    @FunctionalInterface
    private interface StreamWriter {
        void write(DataOutputStream stream) throws IOException;
    }

    private void sendCommand(StreamWriter writer) {
        if (out == null) return;
        synchronized (out) {
            try {
                writer.write(out);
                out.flush();
            } catch (IOException ignored) {}
        }
    }

    private void requestServerFiles() {
        sendCommand(stream -> stream.writeUTF("GET_SERVER_FILES"));
    }

    private void showAuthDialog() {
        JDialog dialog = new JDialog(this, "Аккаунт", true);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        JTabbedPane tabs = new JTabbedPane();

        JTextField loginUserField = new JTextField(loginField.getText(), 20);
        JPasswordField loginPassField = new JPasswordField(20);
        JButton loginSubmit = new JButton("Войти");
        JPanel loginPanel = new JPanel();
        loginPanel.setLayout(new BoxLayout(loginPanel, BoxLayout.Y_AXIS));
        loginPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        loginPanel.add(new JLabel("Логин / ID"));
        loginPanel.add(Box.createVerticalStrut(6));
        loginPanel.add(loginUserField);
        loginPanel.add(Box.createVerticalStrut(8));
        loginPanel.add(new JLabel("Пароль"));
        loginPanel.add(Box.createVerticalStrut(6));
        loginPanel.add(loginPassField);
        loginPanel.add(Box.createVerticalStrut(10));
        loginPanel.add(loginSubmit);

        JTextField regUserField = new JTextField(loginField.getText(), 20);
        JPasswordField regPassField = new JPasswordField(20);
        JButton registerSubmit = new JButton("Регистрация");

        JPanel regPanel = new JPanel();
        regPanel.setLayout(new BoxLayout(regPanel, BoxLayout.Y_AXIS));
        regPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        regPanel.add(new JLabel("Логин"));
        regPanel.add(Box.createVerticalStrut(6));
        regPanel.add(regUserField);
        regPanel.add(Box.createVerticalStrut(10));
        regPanel.add(new JLabel("Пароль"));
        regPanel.add(Box.createVerticalStrut(6));
        regPanel.add(regPassField);
        regPanel.add(Box.createVerticalStrut(10));
        regPanel.add(registerSubmit);

        tabs.addTab("Вход", loginPanel);
        tabs.addTab("Регистрация", regPanel);

        dialog.getContentPane().add(tabs, BorderLayout.CENTER);
        dialog.pack();
        dialog.setLocationRelativeTo(this);

        final boolean[] completed = {false};

        loginSubmit.addActionListener(e -> {
            String user = loginUserField.getText().trim();
            String pass = new String(loginPassField.getPassword());
            if (loginWithCredentials(user, pass)) {
                loginField.setText(username);
                passwordField.setText(pass);
                completed[0] = true;
                dialog.dispose();
            }
        });

        registerSubmit.addActionListener(e -> {
            String user = regUserField.getText().trim();
            String pass = new String(regPassField.getPassword());
            if (registerWithCredentials(user, pass)) {
                loginField.setText(username);
                passwordField.setText(pass);
                completed[0] = true;
                dialog.dispose();
            }
        });

        dialog.addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosed(java.awt.event.WindowEvent e) {
                if (!completed[0]) {
                    closeConnection();
                }
            }
        });

        dialog.setVisible(true);
    }

    private boolean openConnection() {
        try {
            KeyStore ts = KeyStore.getInstance("JKS");
            ts.load(new FileInputStream("clienttruststore.jks"),
                    "197826ASD".toCharArray());

            TrustManagerFactory tmf =
                    TrustManagerFactory.getInstance("SunX509");
            tmf.init(ts);

            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, tmf.getTrustManagers(), null);

            SSLSocketFactory ssf = sc.getSocketFactory();
            socket = (SSLSocket) ssf.createSocket("tcp.cloudpub.ru", 62035);

            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
            return true;
        } catch (IOException | KeyManagementException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            JOptionPane.showMessageDialog(this, "Connection failed");
            return false;
        }
    }

    private void closeConnection() {
        try {
            if (socket != null) socket.close();
        } catch (Exception ignored) {}
        socket = null;
        in = null;
        out = null;
        serverPublicKey = null;
        selfUserId = null;
        currentChatId = null;
        userIdField.setText("");
        friends.clear();
        pinnedChats.clear();
        conversationNamesById.clear();
        conversationCreatorsById.clear();
        updatePinButton();
        updateConversationControls();
    }

    private void finalizeLogin() throws Exception {
        aesKey = AESUtils.generateKey();
        String keyString = AESUtils.keyToString(aesKey);

        if (serverPublicKey != null) {
            String encryptedKey = RSAUtils.encrypt(keyString, serverPublicKey);
            out.writeUTF("AES_KEY_RSA");
            out.writeUTF(encryptedKey);
        } else {
            out.writeUTF("AES_KEY");
            out.writeUTF(keyString);
        }
        out.flush();

        loginBtn.setEnabled(false);
        setControlsEnabled(true);
        updateStatusLabel(true);
        requestUsers();
        requestFriends();
        requestConversations();
        loadPinnedChats();
        rebuildUsersList();
        updatePinButton();
        updateConversationControls();
        currentChatId = null;

        if (selfUserId != null && !selfUserId.isEmpty()) {
            chatArea.append("Connected as " + username + " (ID: " + selfUserId + ")\n");
        } else {
            chatArea.append("Connected as " + username + "\n");
        }
        chatArea.append("Введите ID собеседника и нажмите \"Открыть чат\".\n");
        new Thread(this::listen).start();
    }

    private void readServerPublicKey() throws IOException {
        String type = in.readUTF();
        if (!"RSA_PUBLIC_KEY".equals(type)) {
            throw new IOException("Missing RSA public key from server");
        }
        String keyString = in.readUTF();
        try {
            serverPublicKey = RSAUtils.stringToPublicKey(keyString);
        } catch (Exception e) {
            throw new IOException("Invalid RSA public key", e);
        }
    }

    private boolean loginWithCredentials(String user, String pass) {
        if (user.isEmpty() || pass.isEmpty()) return false;
        if (!openConnection()) return false;
        try {
            out.writeUTF("LOGIN");
            out.writeUTF(user);
            out.writeUTF(pass);
            out.flush();

            String response = in.readUTF();
            if ("LOGIN_FAILED".equals(response)) {
                String reason = in.readUTF();
                JOptionPane.showMessageDialog(this, reason);
                closeConnection();
                return false;
            }
            if ("LOGIN_OK".equals(response)) {
                username = in.readUTF();
                selfUserId = in.readUTF();
                userIdField.setText(selfUserId);
                if (selfUserId != null) {
                    userNamesById.put(selfUserId, username);
                    userStatuses.put(selfUserId, "ONLINE");
                }
                readServerPublicKey();
                finalizeLogin();
                return true;
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Connection failed");
        }
        closeConnection();
        return false;
    }

    private boolean registerWithCredentials(String user, String pass) {
        if (user.isEmpty() || pass.isEmpty()) return false;
        if (!openConnection()) return false;
        try {
            out.writeUTF("REGISTER");
            out.writeUTF(user);
            out.writeUTF(pass);
            out.flush();

            String response = in.readUTF();
            if ("LOGIN_FAILED".equals(response)) {
                String reason = in.readUTF();
                JOptionPane.showMessageDialog(this, reason);
                closeConnection();
                return false;
            }
            if ("LOGIN_OK".equals(response)) {
                username = in.readUTF();
                selfUserId = in.readUTF();
                userIdField.setText(selfUserId);
                if (selfUserId != null) {
                    userNamesById.put(selfUserId, username);
                    userStatuses.put(selfUserId, "ONLINE");
                }
                readServerPublicKey();
                finalizeLogin();
                return true;
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Connection failed");
        }
        closeConnection();
        return false;
    }

    private void sendMessage() {
        try {
            String text = inputField.getText().trim();
            if (text.isEmpty()) return;

            String encrypted = AESUtils.encrypt(text, aesKey);
            String target = currentChatId;
            if (target == null || target.trim().isEmpty()) {
                JOptionPane.showMessageDialog(this, "Введите ID собеседника и откройте чат");
                return;
            }

            sendCommand(stream -> {
                if (isConversationId(target)) {
                    stream.writeUTF("CONVERSATION_MSG");
                    stream.writeUTF(target);
                    stream.writeUTF(encrypted);
                } else {
                    stream.writeUTF("PRIVATE");
                    stream.writeUTF(target);
                    stream.writeUTF(encrypted);
                }
            });
            inputField.setText("");
            
            // Play message sent sound
            playMessageSentSound();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void requestUsers() {
        sendCommand(stream -> stream.writeUTF("GET_USERS"));
    }

    private void requestFriends() {
        sendCommand(stream -> stream.writeUTF("GET_FRIENDS"));
    }

    private void requestConversations() {
        sendCommand(stream -> stream.writeUTF("GET_CONVERSATIONS"));
    }

    private void addFriendById() {
        String targetId = resolveTargetIdInput();
        if (targetId == null) return;
        sendCommand(stream -> {
            stream.writeUTF("ADD_FRIEND");
            stream.writeUTF(targetId);
        });
    }

    private void createConversation() {
        String name = JOptionPane.showInputDialog(this, "Название беседы (можно оставить пустым)");
        if (name == null) return;
        sendCommand(stream -> {
            stream.writeUTF("CREATE_CONVERSATION");
            stream.writeUTF(name);
        });
    }

    private void inviteToConversation() {
        if (!isConversationId(currentChatId)) {
            JOptionPane.showMessageDialog(this, "Сначала откройте беседу");
            return;
        }
        String targetId = resolveTargetIdInput();
        if (targetId == null) return;
        String conversationId = currentChatId;
        sendCommand(stream -> {
            stream.writeUTF("INVITE_TO_CONVERSATION");
            stream.writeUTF(conversationId);
            stream.writeUTF(targetId);
        });
    }

    private void renameConversation() {
        if (!isConversationId(currentChatId)) {
            JOptionPane.showMessageDialog(this, "Сначала откройте беседу");
            return;
        }
        String creator = conversationCreatorsById.get(currentChatId);
        if (creator != null && selfUserId != null && !selfUserId.equals(creator)) {
            JOptionPane.showMessageDialog(this, "Только создатель может менять название");
            return;
        }
        String name = JOptionPane.showInputDialog(this, "Новое название беседы");
        if (name == null) return;
        String conversationId = currentChatId;
        sendCommand(stream -> {
            stream.writeUTF("SET_CONVERSATION_NAME");
            stream.writeUTF(conversationId);
            stream.writeUTF(name);
        });
    }

    private void togglePinnedChat() {
        String targetId = currentChatId;
        if (targetId == null || targetId.trim().isEmpty()) {
            targetId = targetIdField.getText().trim();
        }
        if (targetId == null || targetId.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Сначала откройте чат");
            return;
        }
        if (selfUserId != null && targetId.equals(selfUserId)) {
            JOptionPane.showMessageDialog(this, "Нельзя закрепить себя");
            return;
        }
        if (pinnedChats.contains(targetId)) {
            pinnedChats.remove(targetId);
        } else {
            pinnedChats.add(targetId);
        }
        persistPinnedChats();
        rebuildUsersList();
        updatePinButton();
    }

    private void updatePinButton() {
        if (currentChatId == null || currentChatId.trim().isEmpty()) {
            pinChatBtn.setText("Закрепить");
            pinChatBtn.setEnabled(false);
            return;
        }
        boolean pinned = pinnedChats.contains(currentChatId);
        pinChatBtn.setText(pinned ? "Открепить" : "Закрепить");
        pinChatBtn.setEnabled(true);
    }

    private void updateConversationControls() {
        boolean enabled = sendBtn.isEnabled();
        boolean isConversation = enabled && isConversationId(currentChatId);
        inviteToConversationBtn.setEnabled(isConversation);
        boolean isCreator = isConversation
                && selfUserId != null
                && selfUserId.equals(conversationCreatorsById.get(currentChatId));
        renameConversationBtn.setEnabled(isCreator);
    }

    private boolean isConversationId(String id) {
        return id != null && (id.startsWith("C") || id.startsWith("c"));
    }

    private String normalizeConversationId(String id) {
        if (id == null) return null;
        if (id.startsWith("c")) {
            return "C" + id.substring(1);
        }
        return id;
    }

    private String resolveTargetIdInput() {
        String targetId = targetIdField.getText().trim();
        if (targetId.isEmpty()) {
            String selected = usersList.getSelectedValue();
            if (selected != null && !isConversationId(selected)) {
                targetId = selected;
            }
        }
        if (targetId == null || targetId.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Введите ID собеседника");
            return null;
        }
        if (!targetId.matches("\\d+")) {
            JOptionPane.showMessageDialog(this, "ID должен состоять только из цифр");
            return null;
        }
        if (selfUserId != null && targetId.equals(selfUserId)) {
            JOptionPane.showMessageDialog(this, "Нельзя выбрать себя");
            return null;
        }
        return targetId;
    }

    private void ensureKnownUser(String targetId) {
        if (targetId == null || targetId.trim().isEmpty()) return;
        userStatuses.putIfAbsent(targetId, "OFFLINE");
    }

    private void ensureKnownConversation(String conversationId) {
        if (conversationId == null || conversationId.trim().isEmpty()) return;
        conversationNamesById.putIfAbsent(conversationId, conversationId);
    }

    private void loadPinnedChats() {
        pinnedChats.clear();
        if (selfUserId == null || selfUserId.trim().isEmpty()) return;
        if (!pinnedDb.exists()) return;
        try (BufferedReader reader = new BufferedReader(new FileReader(pinnedDb))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                String[] parts = line.split("\\t", 2);
                if (parts.length < 2) continue;
                if (selfUserId.equals(parts[0].trim())) {
                    String id = parts[1].trim();
                    if (!id.isEmpty()) {
                        pinnedChats.add(id);
                    }
                }
            }
        } catch (IOException ignored) {}
    }

    private void persistPinnedChats() {
        if (selfUserId == null || selfUserId.trim().isEmpty()) return;
        java.util.List<String> retained = new java.util.ArrayList<>();
        if (pinnedDb.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(pinnedDb))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\t", 2);
                    if (parts.length < 2) continue;
                    if (!selfUserId.equals(parts[0].trim())) {
                        retained.add(line);
                    }
                }
            } catch (IOException ignored) {}
        }
        try (FileWriter writer = new FileWriter(pinnedDb, false)) {
            for (String line : retained) {
                writer.write(line + "\n");
            }
            synchronized (pinnedChats) {
                for (String id : pinnedChats) {
                    writer.write(selfUserId + "\t" + id + "\n");
                }
            }
        } catch (IOException ignored) {}
    }

    private void openChatById() {
        String targetId = targetIdField.getText().trim();
        if (targetId.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Введите ID собеседника");
            return;
        }
        targetId = normalizeConversationId(targetId);
        if (isConversationId(targetId)) {
            ensureKnownConversation(targetId);
            rebuildUsersList();
            setActiveChat(targetId);
            return;
        }
        if (!targetId.matches("\\d+")) {
            JOptionPane.showMessageDialog(this, "ID должен состоять только из цифр");
            return;
        }
        if (selfUserId != null && targetId.equals(selfUserId)) {
            JOptionPane.showMessageDialog(this, "Нельзя открыть чат с самим собой");
            return;
        }
        ensureKnownUser(targetId);
        rebuildUsersList();
        setActiveChat(targetId);
    }

    private void setActiveChat(String targetId) {
        if (targetId == null || targetId.isEmpty()) return;
        currentChatId = targetId;
        selectChatInList(targetId);
        requestChatHistory(targetId);
        updatePinButton();
        updateConversationControls();
    }

    private void selectChatInList(String targetId) {
        if (targetId == null) return;
        if (targetId.equals(usersList.getSelectedValue())) return;
        if (usersListModel.contains(targetId)) {
            usersList.setSelectedValue(targetId, true);
        } else {
            usersList.clearSelection();
        }
    }

    private void requestChatHistory(String targetId) {
        if (targetId == null || targetId.trim().isEmpty()) {
            return;
        }
        sendCommand(stream -> {
            if (isConversationId(targetId)) {
                stream.writeUTF("GET_CONVERSATION_HISTORY");
                stream.writeUTF(targetId);
            } else {
                stream.writeUTF("GET_CHAT_HISTORY");
                stream.writeUTF(targetId);
            }
        });
    }

    private String displayNameForId(String id) {
        if (id == null) return "";
        if (isConversationId(id)) {
            String name = conversationNamesById.get(id);
            if (name == null || name.isEmpty()) {
                return id;
            }
            return name + " (" + id + ")";
        }
        String name = userNamesById.get(id);
        if (name == null || name.isEmpty()) {
            return id;
        }
        return id + " (" + name + ")";
    }

    private void rebuildUsersList() {
        SwingUtilities.invokeLater(() -> {
            String selected = currentChatId;
            usersListModel.clear();
            java.util.Set<String> known = new java.util.HashSet<>();
            known.addAll(userNamesById.keySet());
            known.addAll(userStatuses.keySet());
            known.addAll(friends);
            known.addAll(pinnedChats);
            known.addAll(conversationNamesById.keySet());
            if (selfUserId != null) {
                known.remove(selfUserId);
            }

            java.util.List<String> pinned = new java.util.ArrayList<>(pinnedChats);
            pinned.removeIf(id -> selfUserId != null && id.equals(selfUserId));
            pinned.sort(String::compareTo);

            java.util.List<String> conversations = new java.util.ArrayList<>();
            for (String id : conversationNamesById.keySet()) {
                if (pinnedChats.contains(id)) continue;
                conversations.add(id);
            }
            conversations.sort(String::compareTo);

            java.util.List<String> friendList = new java.util.ArrayList<>();
            for (String id : friends) {
                if (pinnedChats.contains(id)) continue;
                if (selfUserId != null && id.equals(selfUserId)) continue;
                if (isConversationId(id)) continue;
                friendList.add(id);
            }
            friendList.sort(String::compareTo);

            java.util.List<String> others = new java.util.ArrayList<>();
            for (String id : known) {
                if (pinnedChats.contains(id) || friends.contains(id) || isConversationId(id)) continue;
                others.add(id);
            }
            others.sort(String::compareTo);

            for (String id : pinned) {
                usersListModel.addElement(id);
            }
            for (String id : conversations) {
                usersListModel.addElement(id);
            }
            for (String id : friendList) {
                usersListModel.addElement(id);
            }
            for (String id : others) {
                usersListModel.addElement(id);
            }

            if (selected != null && usersListModel.contains(selected)) {
                usersList.setSelectedValue(selected, true);
            } else {
                usersList.clearSelection();
            }
        });
    }

    private void sendFile() {
        try {
            JFileChooser chooser = new JFileChooser();
            if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION)
                return;

            File file = chooser.getSelectedFile();
            byte[] data = new byte[(int) file.length()];

            try (FileInputStream fis = new FileInputStream(file)) {
                fis.read(data);
            }

            sendCommand(stream -> {
                stream.writeUTF("FILE");
                stream.writeUTF(file.getName());
                stream.writeLong(data.length);
                stream.write(data);
            });

        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "File send error");
        }
    }

    private void showCallSelector() {
        if (activeCallPeer != null || inCall) {
            JOptionPane.showMessageDialog(this, "Сначала завершите текущий вызов");
            return;
        }
        if (userStatuses.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Пользователи пока не загружены");
            return;
        }
        java.util.List<String> displayNames = new java.util.ArrayList<>();
        java.util.List<String> ids = new java.util.ArrayList<>();
        userStatuses.keySet().stream()
                .filter(id -> selfUserId == null || !id.equals(selfUserId))
                .sorted()
                .forEach(id -> {
                    String status = userStatuses.getOrDefault(id, "OFFLINE");
                    displayNames.add(displayNameForId(id) + " (" + status.toLowerCase() + ")");
                    ids.add(id);
                });

        if (ids.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Нет других пользователей на сервере");
            return;
        }

        String selected = (String) JOptionPane.showInputDialog(
                this,
                "Выберите пользователя для звонка:",
                "Вызов",
                JOptionPane.PLAIN_MESSAGE,
                null,
                displayNames.toArray(new String[0]),
                displayNames.get(0)
        );

        if (selected != null) {
            int index = displayNames.indexOf(selected);
            if (index >= 0) {
                String target = ids.get(index);
                String status = userStatuses.getOrDefault(target, "OFFLINE");
                if (!"ONLINE".equals(status)) {
                    JOptionPane.showMessageDialog(this,
                            displayNameForId(target) + " сейчас " + status.toLowerCase());
                    return;
                }
                initiateCall(target);
            }
        }
    }

    private void initiateCall(String target) {
        try {
            activeCallPeer = target;
            sendCommand(stream -> {
                stream.writeUTF("CALL_REQUEST");
                stream.writeUTF(target);
            });
            chatArea.append("Звонок: ожидается принятие " + displayNameForId(target) + "\n");
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Не удалось инициировать звонок");
            activeCallPeer = null;
        }
    }

    private void handleIncomingCall(String caller) {
        if (inCall || activeCallPeer != null) {
            sendCommand(stream -> {
                stream.writeUTF("CALL_BUSY");
                stream.writeUTF(caller);
            });
            return;
        }
        
        // Play incoming call sound
        playIncomingCallSound();
        
        int answer = JOptionPane.showConfirmDialog(
                this,
                displayNameForId(caller) + " хочет с вами связаться. Принять?",
                "Входящий звонок",
                JOptionPane.YES_NO_OPTION
        );
        if (answer == JOptionPane.YES_OPTION) {
            activeCallPeer = caller;
            sendCommand(stream -> {
                stream.writeUTF("CALL_ACCEPT");
                stream.writeUTF(caller);
            });
        } else {
            sendCommand(stream -> {
                stream.writeUTF("CALL_DECLINE");
                stream.writeUTF(caller);
            });
        }
    }

    private void openCallWindow(String peer) {
        activeCallPeer = peer;
        inCall = true;
        boolean audioReady = startAudioStreamsSafe();
        if (!audioReady) {
            chatArea.append("Аудио устройство недоступно. Завершаем звонок.\n");
            endCall();
            return;
        }
        SwingUtilities.invokeLater(() -> {
            if (callDialog != null) {
                callDialog.dispose();
            }
            callDialog = new JDialog(this, "Голосовой звонок", false);
            callDialog.setLayout(new BorderLayout(10, 10));
            callDialog.add(new JLabel("Говорите с " + displayNameForId(peer), SwingConstants.CENTER), BorderLayout.CENTER);
            JButton endCallBtn = new JButton("Завершить");
            endCallBtn.addActionListener(e -> endCall());
            JPanel footer = new JPanel();
            footer.add(endCallBtn);
            callDialog.add(footer, BorderLayout.SOUTH);
            callDialog.setSize(280, 140);
            callDialog.setLocationRelativeTo(this);
            callDialog.setVisible(true);
            callBtn.setEnabled(false);
            setControlsEnabled(false);
        });
    }

    private void closeCallWindow() {
        stopAudioStreams();
        inCall = false;
        activeCallPeer = null;
        SwingUtilities.invokeLater(() -> {
            if (callDialog != null) {
                callDialog.dispose();
                callDialog = null;
            }
            callBtn.setEnabled(true);
            setControlsEnabled(true);
        });
    }

    private void endCall() {
        if (activeCallPeer == null) return;
        String peer = activeCallPeer;
        stopAudioStreams();
        sendCommand(stream -> {
            stream.writeUTF("CALL_END");
            stream.writeUTF(peer);
        });
        closeCallWindow();
        chatArea.append("Звонок завершён\n");
    }

    private void startAudioStreams() {
        if (capturing) return;
        try {
            DataLine.Info captureInfo = new DataLine.Info(TargetDataLine.class, audioFormat);
            microphone = (TargetDataLine) AudioSystem.getLine(captureInfo);
            microphone.open(audioFormat);
            microphone.start();
        } catch (Exception e) {
            capturing = false;
            microphone = null;
            return;
        }
        capturing = true;
        captureThread = new Thread(() -> {
            byte[] buffer = new byte[1024];
            while (capturing && inCall && microphone != null) {
                try {
                    int read = microphone.read(buffer, 0, buffer.length);
                    if (read > 0) {
                        sendAudioFrame(buffer, read);
                    }
                } catch (Exception e) {
                    break;
                }
            }
        }, "AudioCapture");
        captureThread.setDaemon(true);
        captureThread.start();
        try {
            DataLine.Info playInfo = new DataLine.Info(SourceDataLine.class, audioFormat);
            speaker = (SourceDataLine) AudioSystem.getLine(playInfo);
            speaker.open(audioFormat);
            speaker.start();
        } catch (Exception e) {
            speaker = null;
        }
    }

    private boolean startAudioStreamsSafe() {
        try {
            startAudioStreams();
            return microphone != null || speaker != null;
        } catch (Exception e) {
            stopAudioStreams();
            return false;
        }
    }

    private void stopAudioStreams() {
        capturing = false;
        if (captureThread != null) {
            captureThread.interrupt();
            captureThread = null;
        }
        if (microphone != null) {
            microphone.stop();
            microphone.close();
            microphone = null;
        }
        if (speaker != null) {
            speaker.stop();
            speaker.close();
            speaker = null;
        }
    }

    private void sendAudioFrame(byte[] buffer, int length) {
        if (out == null || activeCallPeer == null) return;
        sendCommand(stream -> {
            stream.writeUTF("AUDIO_FRAME");
            stream.writeInt(length);
            stream.write(buffer, 0, length);
        });
    }

    private void playAudio(byte[] buffer, int length) {
        if (speaker != null) {
            speaker.write(buffer, 0, length);
        }
    }

    private void listen() {
        try {
            while (true) {
                String type = in.readUTF();

                if (type.equals("MSG")) {
                    in.readUTF();
                    in.readUTF();
                    continue;
                } else if (type.equals("PRIVATE")) {
                    String senderId = in.readUTF();
                    String recipientId = in.readUTF();
                    String text = in.readUTF();
                    String otherId = senderId.equals(selfUserId) ? recipientId : senderId;
                    if (otherId != null && otherId.equals(currentChatId)) {
                        if (senderId.equals(selfUserId)) {
                            chatArea.append("[PM] you -> " + displayNameForId(otherId) + ": " + text + "\n");
                        } else {
                            chatArea.append("[PM] " + displayNameForId(otherId) + " -> you: " + text + "\n");
                        }
                    }
                    // Play sound for received message (only if not sent by current user)
                    if (!senderId.equals(selfUserId)) {
                        playMessageReceivedSound();
                    }
                } else if (type.equals("CHAT_HISTORY")) {
                    int count = in.readInt();
                    chatArea.setText("");
                    String header = currentChatId == null ? "Чат" : displayNameForId(currentChatId);
                    chatArea.append("---- " + header + " ----\n");
                    for (int i = 0; i < count; i++) {
                        chatArea.append(in.readUTF() + "\n");
                    }
                    chatArea.append("----------------------\n");
                } else if (type.equals("Ошибка загрузки")) {
                    JOptionPane.showMessageDialog(this, "Файл не найден");
                } else if (type.equals("SERVER_FILES_LIST")) {
                    int count = in.readInt();
                    if (count == 0) {
                        JOptionPane.showMessageDialog(this, "Нет файлов на сервере");
                        continue;
                    }
                    String[] files = new String[count];
                    for (int i = 0; i < count; i++) {
                        files[i] = in.readUTF();
                    }
                    String selected = (String) JOptionPane.showInputDialog(
                            this,
                            "Выберите файлы для скачивания:",
                            "Файлы сервера",
                            JOptionPane.PLAIN_MESSAGE,
                            null,
                            files,
                            files[0]
                    );
                    if (selected != null) {
                        sendCommand(stream -> {
                            stream.writeUTF("DOWNLOAD_FILE");
                            stream.writeUTF(selected);
                        });
                    }
                } else if (type.equals("DOWNLOAD_FILE")) {
                    String fileName = in.readUTF();
                    long size = in.readLong();
                    byte[] data = new byte[(int) size];
                    in.readFully(data);
                    File file = new File(downloadsDir, fileName);
                    try (FileOutputStream fos = new FileOutputStream(file)) {
                        fos.write(data);
                    }
                    JOptionPane.showMessageDialog(this,
                            "Скачено: " + file.getName());
                } else if (type.equals("DOWNLOAD_FAILED")) {
                    JOptionPane.showMessageDialog(this, "Файл не найден");
                } else if (type.equals("FILE")) {
                    String sender = in.readUTF();
                    String fileName = in.readUTF();
                    long size = in.readLong();
                    byte[] data = new byte[(int) size];
                    in.readFully(data);
                    File file =
                            new File(downloadsDir, sender + "_" + fileName);
                    try (FileOutputStream fos = new FileOutputStream(file)) {
                        fos.write(data);
                    }
                    chatArea.append(sender + " sent file: " + file.getName() + "\n");
                } else if (type.equals("USERS_LIST")) {
                    int count = in.readInt();
                    for (int i = 0; i < count; i++) {
                        String id = in.readUTF();
                        String name = in.readUTF();
                        String status = in.readUTF();
                        userStatuses.put(id, status);
                        if (name != null && !name.isEmpty()) {
                            userNamesById.put(id, name);
                        }
                    }
                    rebuildUsersList();
                } else if (type.equals("CONVERSATIONS_LIST")) {
                    int count = in.readInt();
                    conversationNamesById.clear();
                    conversationCreatorsById.clear();
                    for (int i = 0; i < count; i++) {
                        String id = in.readUTF();
                        String name = in.readUTF();
                        String creator = in.readUTF();
                        if (id == null || id.trim().isEmpty()) {
                            continue;
                        }
                        String normalizedId = normalizeConversationId(id);
                        conversationNamesById.put(normalizedId, name == null ? normalizedId : name);
                        if (creator != null && !creator.isEmpty()) {
                            conversationCreatorsById.put(normalizedId, creator);
                        }
                    }
                    rebuildUsersList();
                    updateConversationControls();
                } else if (type.equals("FRIENDS_LIST")) {
                    int count = in.readInt();
                    friends.clear();
                    for (int i = 0; i < count; i++) {
                        String id = in.readUTF();
                        String name = in.readUTF();
                        String status = in.readUTF();
                        friends.add(id);
                        userStatuses.put(id, status);
                        if (name != null && !name.isEmpty()) {
                            userNamesById.put(id, name);
                        }
                    }
                    rebuildUsersList();
                } else if (type.equals("STATUS_UPDATE")) {
                    String id = in.readUTF();
                    String name = in.readUTF();
                    String status = in.readUTF();
                    userStatuses.put(id, status);
                    if (name != null && !name.isEmpty()) {
                        userNamesById.put(id, name);
                    }
                    chatArea.append("[status] " + displayNameForId(id) + " is " + status.toLowerCase() + "\n");
                    rebuildUsersList();
                } else if (type.equals("CONVERSATION_CREATED")) {
                    String id = in.readUTF();
                    String name = in.readUTF();
                    String creator = in.readUTF();
                    String normalizedId = normalizeConversationId(id);
                    conversationNamesById.put(normalizedId, name == null ? normalizedId : name);
                    if (creator != null && !creator.isEmpty()) {
                        conversationCreatorsById.put(normalizedId, creator);
                    }
                    rebuildUsersList();
                    setActiveChat(normalizedId);
                } else if (type.equals("CONVERSATION_ADDED")) {
                    String id = in.readUTF();
                    String name = in.readUTF();
                    String creator = in.readUTF();
                    String normalizedId = normalizeConversationId(id);
                    conversationNamesById.put(normalizedId, name == null ? normalizedId : name);
                    if (creator != null && !creator.isEmpty()) {
                        conversationCreatorsById.put(normalizedId, creator);
                    }
                    rebuildUsersList();
                    JOptionPane.showMessageDialog(this,
                            "Вас добавили в беседу: " + displayNameForId(normalizedId));
                } else if (type.equals("CONVERSATION_NAME_UPDATED")) {
                    String id = in.readUTF();
                    String newName = in.readUTF();
                    String normalizedId = normalizeConversationId(id);
                    if (newName != null && !newName.trim().isEmpty()) {
                        conversationNamesById.put(normalizedId, newName);
                    }
                    rebuildUsersList();
                    if (currentChatId != null && currentChatId.equals(normalizedId)) {
                        requestChatHistory(normalizedId);
                    }
                } else if (type.equals("CONVERSATION_MSG")) {
                    String conversationId = normalizeConversationId(in.readUTF());
                    String senderId = in.readUTF();
                    String text = in.readUTF();
                    if (conversationId != null && conversationId.equals(currentChatId)) {
                        chatArea.append(displayNameForId(senderId) + ": " + text + "\n");
                    }
                    // Play sound for received conversation message (only if not sent by current user)
                    if (!senderId.equals(selfUserId)) {
                        playMessageReceivedSound();
                    }
                } else if (type.equals("CONVERSATION_HISTORY")) {
                    String conversationId = normalizeConversationId(in.readUTF());
                    int count = in.readInt();
                    if (conversationId != null && conversationId.equals(currentChatId)) {
                        chatArea.setText("");
                        String header = displayNameForId(conversationId);
                        chatArea.append("---- " + header + " ----\n");
                        for (int i = 0; i < count; i++) {
                            chatArea.append(in.readUTF() + "\n");
                        }
                        chatArea.append("----------------------\n");
                    } else {
                        for (int i = 0; i < count; i++) {
                            in.readUTF();
                        }
                    }
                } else if (type.equals("CONVERSATION_FAILED")) {
                    String reason = in.readUTF();
                    JOptionPane.showMessageDialog(this, reason);
                } else if (type.equals("FRIEND_ADDED")) {
                    String id = in.readUTF();
                    String name = in.readUTF();
                    friends.add(id);
                    if (name != null && !name.isEmpty()) {
                        userNamesById.put(id, name);
                    }
                    userStatuses.putIfAbsent(id, "OFFLINE");
                    rebuildUsersList();
                    JOptionPane.showMessageDialog(this,
                            "Добавлен в друзья: " + displayNameForId(id));
                } else if (type.equals("FRIEND_FAILED")) {
                    String reason = in.readUTF();
                    JOptionPane.showMessageDialog(this, reason);
                } else if (type.equals("AUDIO_FRAME")) {
                    int length = in.readInt();
                    byte[] audio = new byte[length];
                    in.readFully(audio);
                    playAudio(audio, length);
                } else if (type.equals("CALL_INVITE")) {
                    String caller = in.readUTF();
                    handleIncomingCall(caller);
                } else if (type.equals("CALL_ESTABLISHED")) {
                    String peer = in.readUTF();
                    activeCallPeer = peer;
                    openCallWindow(peer);
                    chatArea.append("Звонок с " + displayNameForId(peer) + " начат\n");
                } else if (type.equals("CALL_ENDED")) {
                    String peer = in.readUTF();
                    stopAudioStreams();
                    closeCallWindow();
                    chatArea.append("Звонок с " + displayNameForId(peer) + " завершён\n");
                } else if (type.equals("CALL_DECLINED")) {
                    String peer = in.readUTF();
                    chatArea.append(displayNameForId(peer) + " отклонил звонок\n");
                    activeCallPeer = null;
                } else if (type.equals("CALL_BUSY")) {
                    String peer = in.readUTF();
                    chatArea.append(displayNameForId(peer) + " занят\n");
                    activeCallPeer = null;
                } else if (type.equals("PRIVATE_FAILED")) {
                    String reason = in.readUTF();
                    JOptionPane.showMessageDialog(this, reason);
                }
            }
        } catch (Exception e) {
            chatArea.append("Отключено\n");
            updateStatusLabel(false);
            setControlsEnabled(false);
            downloadBtn.setEnabled(false);
            loginBtn.setEnabled(true);
            userNamesById.clear();
            userStatuses.clear();
            friends.clear();
            pinnedChats.clear();
            conversationNamesById.clear();
            conversationCreatorsById.clear();
            rebuildUsersList();
            updatePinButton();
            updateConversationControls();
        }
    }

    public static void main(String[] args) {
        try {
            for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
            if (!"Nimbus".equals(UIManager.getLookAndFeel().getName())) {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            }
        } catch (Exception ignored) {}
        SwingUtilities.invokeLater(() -> new Client().setVisible(true));
    }
}