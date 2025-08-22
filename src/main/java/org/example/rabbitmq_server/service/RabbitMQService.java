package org.example.rabbitmq_server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.example.rabbitmq_server.model.User;
import org.example.SQlite.DataBaseService;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class RabbitMQService {
    private String managementUrl = "http://localhost:15672/api";
    private int portNo = 5672;
    private static final String DEFAULT_USERNAME = "guest";
    private static final String DEFAULT_PASSWORD = "guest";

    private DataBaseService dataBaseService;

    private final ObjectMapper mapper;
    private String currentUser;
    private String currentPassword;

    public RabbitMQService() {
        this.mapper = new ObjectMapper();
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); // Bilinmeyen JSON alanları için hata verme
        this.mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false); // null olanlar var ise hata verme
        this.mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true); // Boş arrayleri null olarak kabul et
        
        this.currentUser = DEFAULT_USERNAME;
        this.currentPassword = DEFAULT_PASSWORD;

        this.dataBaseService = new DataBaseService();
    }
    
    public void setCredentials(String username, String password) {
        this.currentUser = username;
        this.currentPassword = password;
    }
    
    private String getAuthHeader() {
        String credentials = currentUser + ":" + currentPassword;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());
    }

    // RabbitMQ API ile bağlantı kurma metotu
    public boolean connection() {
        try {
            java.net.URL url = new java.net.URL(managementUrl + "/overview");
            java.net.HttpURLConnection connection = (java.net.HttpURLConnection) url.openConnection();
            
            String credentials = currentUser + ":" + currentPassword;
            String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes());
            connection.setRequestProperty("Authorization", "Basic " + encodedCredentials);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            
            int responseCode = connection.getResponseCode();
            connection.disconnect();
            
            return responseCode == 200 || responseCode == 401;
            
        } catch (Exception e) {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost("localhost");
                factory.setPort(this.portNo);
                factory.setUsername(currentUser);
                factory.setPassword(currentPassword);
                factory.setConnectionTimeout(3000);
                
                try (Connection connection = factory.newConnection()) {
                    return connection.isOpen();
                }
            } catch (Exception amqpException) {
                return false;
            }
        }
    }
    // Server başlatma, durdurma ve restart işlemleri
    public void startRabbitMQServer() throws Exception {
        try{
            if(isRabbitMQServiceRunning()){
                return;
            }

            String[] commands = {
                    "powershell", "-Command",
                    "Start-Process", "cmd",
                    "-ArgumentList", "'/c net start RabbitMQ",
                    "-Verb", "RunAs",
                    "-WindowStyle", "Hidden"
            };

            ProcessBuilder processBuilder = new ProcessBuilder(commands);
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();

            int exitCode = process.waitFor();
            if(exitCode == 0){
                Thread.sleep(5000);
                return;
            }

            String[] altCommands = {
                    "powershell", "-Command",
                    "Start-Process", "cmd",
                    "-ArgumentList", "'/c sc start RabbitMQ'",
                    "-Verb", "RunAs",
                    "-WindowStyle", "Hidden"
            };

            processBuilder = new ProcessBuilder(altCommands);
            processBuilder.redirectErrorStream(true);
            process = processBuilder.start();

            exitCode = process.waitFor();
            if(exitCode == 0){
                Thread.sleep(5000);
                return;
            }
        }catch (Exception e){
            throw new Exception("RabbitMQ başlatılamadı. Yönetici izni gerekli.");
        }
    }

    public void stopRabbitMQServer() throws Exception {
        try {
            if (!isRabbitMQServiceRunning()) {
                return;
            }

            String[] commands = {
                    "powershell", "-Command",
                    "Start-Process", "cmd",
                    "-ArgumentList", "'/c net stop RabbitMQ'",
                    "-Verb", "RunAs",
                    "-WindowStyle", "Hidden"
            };

            ProcessBuilder processBuilder = new ProcessBuilder(commands);
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();

            int exitCode = process.waitFor();
            if (exitCode == 0) {
                return;
            }

            String[] altCommands = {
                    "powershell", "-Command",
                    "Start-Process", "cmd",
                    "-ArgumentList", "'/c sc stop RabbitMQ'",
                    "-Verb", "RunAs",
                    "-WindowStyle", "Hidden"
            };

            processBuilder = new ProcessBuilder(altCommands);
            processBuilder.redirectErrorStream(true);
            process = processBuilder.start();
            process.waitFor();

        } catch (Exception e) {
            throw new Exception("RabbitMQ durdurulamadı. Yönetici izni gerekli.");
        }
    }


    public boolean restartRabbitMQSafely() {
        try {
            System.out.println("RabbitMQ yeniden başlatılıyor...");

            if (isRabbitMQServiceRunning()) {
                stopRabbitMQServer();
                Thread.sleep(3000);
            }

            startRabbitMQServer();
            Thread.sleep(5000);

            if (connection()) {
                System.out.println("RabbitMQ başarıyla yeniden başlatıldı!");
                return true;
            } else {
                System.out.println("RabbitMQ başlatıldı ama bağlantı kurulamadı.");
                return false;
            }

        } catch (Exception e) {
            System.out.println("Yeniden başlatma hatası: " + e.getMessage());
            return false;
        }
    }

    // İzin ayarlama metodu ("^$" bu şekilde bulunan regex izin ayarlarında belirtilen ayar üzerinden iznin olmadığını belirtir)
    public void setPermissions(String username ,String configure, String write, String read) throws Exception {
            try{
                if(configure.isEmpty()|| configure.trim().isEmpty()){
                    configure = "^$";
                }
                if(write.isEmpty()|| write.trim().isEmpty()){
                    write = "^$";
                }
                if(read.isEmpty()|| read.trim().isEmpty()){
                    read = "^$";
                }

                String command = String.format("rabbitmqctl set_permissions -p / %s \"%s\" \"%s\" \"%s\"",
                        username,configure, write, read);

                String[] perCommands = {
                        "powershell", "-Command",
                        "Start-Process", "cmd",
                        "-ArgumentList", "'/c " + command + "'",
                        "-Verb", "RunAs",
                        "-WindowStyle", "Hidden"
                };

                ProcessBuilder processBuilder = new ProcessBuilder(perCommands);
                processBuilder.redirectErrorStream(true);
                Process process = processBuilder.start();
                process.waitFor();
            }catch (Exception e){
                throw new Exception("Kullanıcı yetkilendirmede hata oluştu: " + e.getMessage());
            }
    }

    
    private boolean isRabbitMQServiceRunning() {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("sc", "query", "RabbitMQ");
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
            
                         Scanner scanner = new Scanner(process.getInputStream());
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.contains("STATE") && line.contains("RUNNING")) {
                    scanner.close();
                    return true;
                }
            }
            scanner.close();
            return false;
        } catch (Exception e) {
            System.out.println("Service durumu kontrol edilemedi: " + e.getMessage());
            return false;
        }
    }

    // Tüm kullanıcıları getiren metot
    public List<User> getUsers() throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(managementUrl + "/users");
            httpGet.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());

            return httpClient.execute(httpGet, response -> {
                if (response.getCode() == 200) {
                    try {
                        String json = new String(response.getEntity().getContent().readAllBytes());
                        System.out.println("RabbitMQ API Response: " + json);
                        List<User> userList = mapper.readValue(json, new TypeReference<List<User>>() {});

                        Map<String, DataBaseService.UserSessionInfo> sessionInfoMap = dataBaseService.getAllUserSessions();

                        for (User user : userList) {
                            DataBaseService.UserSessionInfo sessionInfo = sessionInfoMap.get(user.getName());
                            if (sessionInfo != null) {
                                user.setLastLoginFormatted(sessionInfo.lastLoginFormatted);
                                user.setLastLogoutFormatted(sessionInfo.lastLogoutFormatted);
                                user.setSessionDurationFormatted(sessionInfo.sessionDurationFormatted);
                                user.setActiveSession(sessionInfo.isActiveSession);
                                user.setLoginStartTime(sessionInfo.activeSessionStartTime);
                            } else {
                                user.setLastLoginFormatted("Giriş Yapmadı");
                                user.setLastLogoutFormatted("Çıkış Yapmadı");
                                user.setSessionDurationFormatted("0 dk");
                                user.setActiveSession(false);
                                user.setLoginStartTime(null);
                            }
                        }

                        return userList;
                    } catch (Exception e) {
                        throw new IOException("JSON çözümleme hatası: " + e.getMessage(), e);
                    }
                } else if (response.getCode() == 401) {
                    throw new IOException("Yetki hatası: Kullanıcı adı veya şifre yanlış");
                } else if (response.getCode() == 403) {
                    throw new IOException("İzin hatası: Bu işlem için yetkiniz yok");
                } else {
                    throw new IOException("API Hatası: " + response.getCode() + " " + response.getReasonPhrase());
                }
            });
        } catch (Exception e) {
            throw new Exception("RabbitMQ bağlantı hatası: " + e.getMessage(), e);
        }
    }
    // Kullanıcı oluşturma ve silme metotları
    public void createUser(String username, String password, String tags) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPut httpPut = new HttpPut(managementUrl + "/users/" + username);
            httpPut.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
            httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

            Map<String, Object> userData = new HashMap<>();
            userData.put("password", password);
            userData.put("tags", tags);

            String json = mapper.writeValueAsString(userData);
            httpPut.setEntity(new StringEntity(json));

            httpClient.execute(httpPut, response -> {
                int code = response.getCode();
                if (code != 200 && code != 201 && code != 204) {
                    throw new IOException("API Hatas<UNK>: " + code + " " + response.getReasonPhrase());
                }
                return null;
            });
        }
    }
    
    public void deleteUser(String username) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpDelete httpDelete = new HttpDelete(managementUrl + "/users/" + username);
            httpDelete.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
            
            httpClient.execute(httpDelete, response -> {
                if (response.getCode() != 204) {
                    throw new IOException("Kullanıcı silinemedi: " + response.getCode() + " " + response.getReasonPhrase());
                }
                return null;
            });
        }
    }

    // Kullanıcının admin olup olmadığını kontrol etme
    public boolean isCurrentUserAdmin() {
        try {
            List<User> users = getUsers();
            return users.stream()
                    .filter(user -> user.getName().equals(currentUser))
                    .findFirst()
                    .map(User::isAdmin)
                    .orElse(false);
        } catch (Exception e) {
            return false;
        }
    }

    // Port kısıtlama metotları
    public boolean setRabbitMQPortSafely(int port) {
        if (port < 1024 || port > 65535) {
            System.out.println("Geçersiz port: " + port + " (1024-65535 arası olmalı)");
            return false;
        }
        
        if (port == 4369) {
            System.out.println("Port 4369 epmd için ayrılmış, kullanılamaz!");
            return false;
        }
        
        this.portNo = port;
        System.out.println("Port güncellendi: " + port);
        return true;
    }

    public boolean setRabbitMQSslPortSafely(int sslPort) {
        if (sslPort < 1024 || sslPort > 65535) {
            System.out.println("Geçersiz port: " + sslPort + " (1024-65535 arası olmalı)");
            return false;
        }

        if (sslPort == 4369) {
            System.out.println("Port 4369 epmd için ayrılmış, kullanılamaz!");
            return false;
        }
        if(sslPort == 5672 || sslPort == 5673){
            System.out.println("Port 5672 ve 5673 AMQP için ayrılmıştır!");
        }

        this.portNo = sslPort;
        System.out.println("Port güncellendi: " + sslPort);
        return true;
    }
    
    public boolean setRabbitMQManagementPortSafely(int managementPort) {
        if (managementPort < 1024 || managementPort > 65535) {
            System.out.println("Geçersiz management port: " + managementPort);
            return false;
        }
        
        this.managementUrl = "http://localhost:" + managementPort + "/api";
        System.out.println("Management port güncellendi: " + managementPort);
        return true;
    }

    public void setRabbitMQPort(int port) {
       this.portNo = port;
    }

    public void setRabbitMQManagementPort(int managementPort) {
        this.managementUrl = "http://localhost:" + managementPort + "/api";
    }

    //DataBaseService Metotları

    public void recordUserLogin(String username, String password) {
        dataBaseService.recordUserLogin(username, password);
    }

    public void recordUserLogout(String username) {
        dataBaseService.recordUserLogOut(username);
    }

    public String getLastLogin(String username) {
        return dataBaseService.getLastLogin(username);
    }

    public String getLastLogout(String username) {
        return dataBaseService.getLastLogOut(username);
    }

    public boolean hasActiveSession(String username) {
        return dataBaseService.hasActiveSession(username);
    }

    public void printUserSessionsHistory(String username) {
        dataBaseService.printUserSessionsHistory(username);
    }

    public DataBaseService getDataBaseService() {
        return dataBaseService;
    }

    public void updateSessionDurationInDatabase() {
        dataBaseService.updateSessionDurationInDatabase();
    }
}
