package com.example;

import com.example.db.GateDatabase;
import com.example.model.XeTransaction;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.net.URI;
/**
 * ================================================================
 *  CỔNG BÃI ĐẬU XE - CÓ ĐỒNG THUẬN LAMPORT
 * ================================================================
 *
 *  Deploy lên Render.com với các biến môi trường:
 *  ┌─────────────────────────────────────────────────────────┐
 *  │  GATE_ID      = "1" (hoặc "2", "3")                    │
 *  │  PORT         = 8080                                    │
 *  │  DB_URL       = jdbc:postgresql://...                   │
 *  │  DB_USER      = avnadmin                               │
 *  │  DB_PASSWORD  = ...                                     │
 *  │  PEER_SERVERS = https://gate2.onrender.com,            │
 *  │                 https://gate3.onrender.com             │
 *  └─────────────────────────────────────────────────────────┘
 *
 *  API Endpoints:
 *  GET  /                  → Giao diện web
 *  POST /api/nhan-xe       → Xe vào bãi (từ frontend)
 *  POST /api/p2p/propose   → Nhận PROPOSE từ server khác
 *  POST /api/p2p/ack       → Nhận ACK từ server khác
 *  POST /api/p2p/commit    → Nhận COMMIT từ server khác
 *  GET  /api/trang-thai    → Trạng thái đồng thuận (JSON)
 *  GET  /api/lich-su-db    → Lịch sử từ Database
 * ================================================================
 */
public class GateMain {

    /** Lịch sử sự kiện hiển thị lên giao diện */
    public static final List<String> lichSu = new CopyOnWriteArrayList<>();
    public static LamportConsensus consensus;
    private static GateDatabase db;
    private static String GATE_ID;
    private static final Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        Properties props = loadProperties();

        // ── 1. LẤY ID CỔNG ──────────────────────────────────────
        GATE_ID = firstNonBlank(System.getenv("GATE_ID"), props.getProperty("gate.id"), "1");
        System.out.println("════════════════════════════════════════");
        System.out.println("  Bãi Đậu Xe Phân Tán - Cổng " + GATE_ID);
        System.out.println("  Thuật toán: Đồng Thuận Lamport");
        System.out.println("════════════════════════════════════════");

        // ── 2. KẾT NỐI DATABASE ─────────────────────────────────
        String jdbcUrl = firstNonBlank(System.getenv("DB_URL"), System.getenv("JDBC_URL"),
                                       props.getProperty("db.url"));
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            System.err.println("❌ LỖI: Chưa cấu hình DB_URL! Hãy set biến môi trường.");
            System.exit(1);
        }

        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(jdbcUrl.trim());
        String dbUser = firstNonBlank(System.getenv("DB_USER"), props.getProperty("db.user"));
        String dbPass = firstNonBlank(System.getenv("DB_PASSWORD"), props.getProperty("db.password"));
        if (dbUser != null) hc.setUsername(dbUser);
        if (dbPass  != null) hc.setPassword(dbPass);
        hc.setMaximumPoolSize(5);
        hc.setConnectionTimeout(10_000);

        HikariDataSource ds = new HikariDataSource(hc);
        db = new GateDatabase(ds);
        db.runSchema();
        System.out.println("✅ Kết nối Database thành công!");

        // ── 3. DANH SÁCH PEER SERVERS ────────────────────────────
        String peerEnv = firstNonBlank(System.getenv("PEER_SERVERS"), props.getProperty("peer.servers"), "");
        List<String> peerUrls = new ArrayList<>();
        if (peerEnv != null && !peerEnv.isBlank()) {
            for (String p : peerEnv.split(",")) {
                String url = p.trim();
                if (!url.isEmpty()) peerUrls.add(url);
            }
        }
        System.out.println("🌐 Peer servers: " + (peerUrls.isEmpty() ? "(không có - chế độ đơn)" : peerUrls));

        // ── 4. KHỞI TẠO ĐỒNG THUẬN LAMPORT ──────────────────────
        consensus = new LamportConsensus(GATE_ID, peerUrls, db, lichSu);

        // Khôi phục trạng thái từ DB (sau khi restart)
        long maxClock = db.getMaxLampClock();
        long maxSeq   = db.getMaxGlobalSeq();
        consensus.khoiPhuc(maxClock, maxSeq);

        // ── 5. KHỞI TẠO WEB SERVER ───────────────────────────────
        int port = parseInt(System.getenv("PORT"), 8080);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 100);
        server.setExecutor(Executors.newFixedThreadPool(10));

        // ─── API: Giao diện web ───────────────────────────────────
        server.createContext("/", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) return;
            try (InputStream is = GateMain.class.getClassLoader().getResourceAsStream("index.html")) {
                if (is != null) {
                    byte[] bytes = is.readAllBytes();
                    ex.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
                    sendResponse(ex, 200, bytes);
                } else {
                    String html = "<h1>Cổng " + GATE_ID + " - Đồng thuận Lamport</h1>" +
                                  "<p style='color:red'>Lỗi: Thiếu file index.html!</p>";
                    sendResponse(ex, 200, html.getBytes(StandardCharsets.UTF_8));
                }
            }
        });

        // ─── API 1: Xe vào bãi (từ người dùng / frontend) ─────────
        server.createContext("/api/nhan-xe", ex -> {
            addCors(ex);
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                sendResponse(ex, 204, new byte[0]); return;
            }
            if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
                String bienSo = readBody(ex).trim().toUpperCase();
                if (!bienSo.isEmpty()) {
                    // Khởi động quá trình đồng thuận Lamport!
                    new Thread(() -> consensus.khoiTaoGiaoDich(bienSo)).start();
                    sendResponse(ex, 200, "OK".getBytes());
                } else {
                    sendResponse(ex, 400, "Biển số rỗng".getBytes());
                }
            }
        });

        // ─── API P2P 1: Nhận PROPOSE từ server khác ───────────────
        server.createContext("/api/p2p/propose", ex -> {
            if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
                String body = readBody(ex);
                XeTransaction tx = gson.fromJson(body, XeTransaction.class);

                // Xử lý PROPOSE và lấy ACK để gửi lại
                String ackJson = consensus.nhanPropose(tx);

                // Gửi ACK về cho originator
                String originUrl = findPeerUrl(tx.originGateId, peerUrls);
                if (originUrl != null) {
                    final String url = originUrl;
                    final String ack = ackJson;
                    new Thread(() -> guiHttp(url + "/api/p2p/ack", ack)).start();
                }

                sendResponse(ex, 200, "ACK_SENT".getBytes());
            }
        });

        // ─── API P2P 2: Nhận ACK từ server khác ───────────────────
        server.createContext("/api/p2p/ack", ex -> {
            if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
                String body = readBody(ex);
                XeTransaction ackTx = gson.fromJson(body, XeTransaction.class);
                consensus.nhanAck(ackTx);
                sendResponse(ex, 200, "OK".getBytes());
            }
        });

        // ─── API P2P 3: Nhận COMMIT từ server khác ─────────────────
        server.createContext("/api/p2p/commit", ex -> {
            if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
                String jobId = readBody(ex).trim();
                // Xử lý commit trong luồng riêng để không block HTTP
                new Thread(() -> consensus.xuLyCommit(jobId)).start();
                sendResponse(ex, 200, "OK".getBytes());
            }
        });

        // ─── API: Lấy trạng thái đồng thuận (JSON) ─────────────────
        server.createContext("/api/trang-thai", ex -> {
            addCors(ex);
            if ("GET".equalsIgnoreCase(ex.getRequestMethod())) {
                Map<String, Object> state = new LinkedHashMap<>();
                state.put("gateId",      GATE_ID);
                state.put("lampClock",   consensus.getLampClock());
                state.put("nextSeq",     consensus.getNextGlobalSeq());
                state.put("peerCount",   peerUrls.size());
                state.put("pendingCount", consensus.getPendingList().size());
                state.put("pendingList", consensus.getPendingList().stream()
                    .map(t -> Map.of(
                        "jobId",  t.jobId,
                        "bienSo", t.bienSo,
                        "gate",   t.originGateId,
                        "clock",  t.lampClock,
                        "phase",  t.phase
                    )).toList());
                state.put("lichSu", lichSu.subList(0, Math.min(lichSu.size(), 30)));

                byte[] bytes = gson.toJson(state).getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
                sendResponse(ex, 200, bytes);
            }
        });

        // ─── API: Lịch sử từ Database ───────────────────────────────
        server.createContext("/api/lich-su-db", ex -> {
            addCors(ex);
            if ("GET".equalsIgnoreCase(ex.getRequestMethod())) {
                try {
                    List<Map<String, Object>> rows = db.getXeLog(50);
                    byte[] bytes = gson.toJson(rows).getBytes(StandardCharsets.UTF_8);
                    ex.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
                    sendResponse(ex, 200, bytes);
                } catch (Exception e) {
                    sendResponse(ex, 500, ("Lỗi DB: " + e.getMessage()).getBytes());
                }
            }
        });

        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(ds::close));
        System.out.println("🚀 Cổng " + GATE_ID + " đang lắng nghe tại port " + port);
        System.out.println("   Truy cập: http://localhost:" + port);
    }

    // ════════════════════════════════════════════════════════════
    //  HÀM TIỆN ÍCH
    // ════════════════════════════════════════════════════════════

    /** Tìm URL của peer dựa vào gateId (heuristic: tìm trong URL) */
    private static String findPeerUrl(String targetGateId, List<String> peers) {
        // Thử tìm theo gate ID trong URL (vd: gate1.onrender.com)
        for (String url : peers) {
            if (url.contains("gate" + targetGateId) || url.contains("-" + targetGateId)) {
                return url;
            }
        }
        // Nếu không tìm thấy theo tên, gửi đến tất cả (broadcast fallback)
        // Trong thực tế, nên cấu hình PEER_MAP cụ thể hơn
        return peers.isEmpty() ? null : peers.get(0);
    }

    private static void guiHttp(String url, String body) {
        try {
            var client = java.net.http.HttpClient.newHttpClient();
            var request = java.net.http.HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .timeout(java.time.Duration.ofSeconds(5))
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                    .build();
            client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            System.err.println("[P2P WARN] Gửi HTTP thất bại tới " + url + ": " + e.getMessage());
        }
    }

    private static String readBody(HttpExchange ex) throws IOException {
        return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void sendResponse(HttpExchange ex, int code, byte[] body) throws IOException {
        ex.sendResponseHeaders(code, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private static void addCors(HttpExchange ex) {
        ex.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        ex.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");
    }

    private static Properties loadProperties() throws IOException {
        Properties props = new Properties();
        try (InputStream in = GateMain.class.getClassLoader()
                                            .getResourceAsStream("application.properties")) {
            if (in != null) props.load(in);
        }
        return props;
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    private static int parseInt(String s, int def) {
        try { return s != null ? Integer.parseInt(s.trim()) : def; }
        catch (NumberFormatException e) { return def; }
    }
}