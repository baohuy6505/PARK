package com.example;

import com.example.db.GateDatabase;
import com.example.model.XeTransaction;
import com.google.gson.Gson;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ================================================================
 *  ĐỘNG CƠ ĐỒNG THUẬN LAMPORT (LAMPORT CONSENSUS ENGINE)
 * ================================================================
 *
 *  Giải quyết bài toán: "Làm sao 3 Server ghi dữ liệu vào CSDL
 *  theo ĐÚNG 1 THỨ TỰ DUY NHẤT?"
 *
 *  THUẬT TOÁN (theo slide thầy Vĩ - Giải thuật cơ bản):
 *  ───────────────────────────────────────────────────────
 *
 *  Pha 1 - PROPOSE (Vòng 1: Khóa + Gắn đồng hồ):
 *    → Cổng nhận xe, tăng đồng hồ Lamport: L = ++clock
 *    → Tạo giao dịch tx = {jobId, bienSo, gateId, L}
 *    → Lưu vào pendingMap (bảng tạm)
 *    → Broadcast tx đến TẤT CẢ các Server khác
 *
 *  Pha 2 - ACK (Vòng 2: Cập nhật bảng tạm + Xác nhận):
 *    → Server nhận PROPOSE: cập nhật đồng hồ = max(local, L) + 1
 *    → Thêm tx vào pendingMap của mình
 *    → Gửi ACK về cho Server khởi tạo
 *
 *  Pha 3 - COMMIT (Vòng 3: Kiểm tra đồng bộ + Xác định thứ tự):
 *    → Khi Server khởi tạo nhận đủ ACK từ mọi Server:
 *    → Broadcast COMMIT{jobId} đến TẤT CẢ
 *    → Tất cả Server nhận COMMIT, đánh dấu tx là "sẵn sàng ghi"
 *
 *  Pha 4 - WRITE (Vòng 4: Ghi vào CSDL chính):
 *    → Sắp xếp tất cả tx đã COMMIT theo (lampClock, gateId)
 *    → Ghi vào CSDL theo thứ tự đó
 *    → Vì tất cả Server dùng CÙNG THUẬT TOÁN SẮP XẾP
 *       → Thứ tự ghi vào DB hoàn toàn GIỐNG NHAU ở mọi Server!
 *
 *  KẾT QUẢ: Dữ liệu 3 máy KHỚP NHAU y xì đúc!
 * ================================================================
 */
public class LamportConsensus {

    // ─── Trạng thái nội bộ ───────────────────────────────────────

    /**
     * ĐỒNG HỒ LAMPORT - Tăng dần, không bao giờ giảm.
     * Mỗi Server giữ một đồng hồ riêng nhưng chúng được đồng bộ
     * qua các tin nhắn PROPOSE.
     */
    private final AtomicLong lampClock = new AtomicLong(0);

    /**
     * Bảng tạm trong bộ nhớ: jobId → XeTransaction
     * Chứa các giao dịch ĐANG CHỜ đồng thuận (phase = PROPOSE hoặc ACK)
     */
    private final ConcurrentHashMap<String, XeTransaction> pendingMap = new ConcurrentHashMap<>();

    /**
     * Theo dõi ACK: jobId → Set<gateId đã ACK>
     * Khi set.size() == tổng số peers → đủ điều kiện COMMIT
     */
    private final ConcurrentHashMap<String, Set<String>> ackMap = new ConcurrentHashMap<>();

    /**
     * Hàng đợi ĐÃ COMMIT: TreeSet tự sắp xếp theo (lampClock, gateId)
     * Đây là "hàng đợi 2" theo slide thầy - dùng để ghi vào CSDL chính
     */
    private final TreeSet<XeTransaction> committedQueue = new TreeSet<>();

    /**
     * Tập các jobId đã được ghi vào CSDL (tránh ghi 2 lần)
     */
    private final Set<String> writtenJobIds = ConcurrentHashMap.newKeySet();

    /**
     * Sequence toàn cục - số thứ tự ghi vào CSDL
     * Mỗi Server duy trì counter này - nếu thuật toán đúng, chúng sẽ khớp nhau!
     */
    private long nextGlobalSeq = 1;

    // ─── Cấu hình ────────────────────────────────────────────────
    private final String myGateId;
    private final List<String> peerUrls; // URL của các server khác (không kể self)
    private final GateDatabase db;
    private final List<String> lichSu;  // Log hiển thị lên giao diện
    private final Gson gson = new Gson();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    // ─── Constructor ─────────────────────────────────────────────
    public LamportConsensus(String myGateId, List<String> peerUrls,
                            GateDatabase db, List<String> lichSu) {
        this.myGateId  = myGateId;
        this.peerUrls  = peerUrls;
        this.db        = db;
        this.lichSu    = lichSu;
    }

    /**
     * Khôi phục trạng thái sau khi restart từ DB
     */
    public void khoiPhuc(long maxLampClock, long maxGlobalSeq) {
        lampClock.set(maxLampClock);
        nextGlobalSeq = maxGlobalSeq + 1;
        log("Khôi phục: đồng hồ Lamport=" + maxLampClock + ", seq tiếp theo=" + nextGlobalSeq);
    }

    // ════════════════════════════════════════════════════════════
    //  PHA 1 - PROPOSE: Cổng nhận xe, khởi động quá trình đồng thuận
    // ════════════════════════════════════════════════════════════

    /**
     * GỌI KHI: Một chiếc xe đến cổng này.
     * HÀNH ĐỘNG: Tạo giao dịch + gắn đồng hồ Lamport + broadcast PROPOSE
     */
    public void khoiTaoGiaoDich(String bienSo) {
        // Bước 1: Tăng đồng hồ Lamport (Lamport Rule: increment on send)
        long clock = lampClock.incrementAndGet();

        // Bước 2: Tạo giao dịch mới
        String jobId = java.util.UUID.randomUUID().toString().substring(0, 12);
        XeTransaction tx = new XeTransaction(jobId, bienSo, myGateId, clock);

        log(String.format("🚗 [PROPOSE|L:%d] Xe %s vào Cổng %s - Khởi động đồng thuận...",
                clock, bienSo, myGateId));

        // Bước 3: Lưu vào bảng tạm (bộ nhớ)
        pendingMap.put(jobId, tx);
        savePendingToDB(tx);

        if (peerUrls.isEmpty()) {
            // Không có server nào khác → tự commit luôn (chế độ đơn)
            log("⚡ Không có peer → Tự commit ngay!");
            xuLyCommit(jobId);
            return;
        }

        // Bước 4: Khởi tạo bộ đếm ACK
        ackMap.put(jobId, ConcurrentHashMap.newKeySet());

        // Bước 5: BROADCAST PROPOSE đến tất cả peer (bất đồng bộ)
        String json = gson.toJson(tx);
        for (String peerUrl : peerUrls) {
            final String url = peerUrl;
            CompletableFuture.runAsync(() -> {
                guiHttp(url + "/api/p2p/propose", json, "PROPOSE→" + url);
            });
        }

        // Timeout an toàn: nếu 8 giây chưa nhận đủ ACK → tự commit
        CompletableFuture.delayedExecutor(8, TimeUnit.SECONDS).execute(() -> {
            if (pendingMap.containsKey(jobId) && !writtenJobIds.contains(jobId)) {
                log("⚠️ [TIMEOUT] Chưa nhận đủ ACK cho " + jobId + " → Tự commit (fault tolerance)");
                xuLyCommit(jobId);
            }
        });
    }

    // ════════════════════════════════════════════════════════════
    //  PHA 2 - ACK: Nhận PROPOSE từ server khác, gửi ACK về
    // ════════════════════════════════════════════════════════════

    /**
     * GỌI KHI: Nhận được thông điệp PROPOSE từ một server khác.
     * HÀNH ĐỘNG: Cập nhật đồng hồ Lamport + thêm vào bảng tạm + gửi ACK
     * TRẢ VỀ: JSON của ACK message để gửi lại cho originator
     */
    public String nhanPropose(XeTransaction tx) {
        // Quy tắc Lamport khi NHẬN: clock = max(local, received) + 1
        long newClock = Math.max(lampClock.get(), tx.lampClock) + 1;
        lampClock.set(newClock);

        log(String.format("📨 [NHẬN PROPOSE|L:%d→%d] Xe %s từ Cổng %s",
                tx.lampClock, newClock, tx.bienSo, tx.originGateId));

        // Lưu vào bảng tạm
        pendingMap.put(tx.jobId, tx);
        savePendingToDB(tx);

        // Tạo ACK: gắn đồng hồ Lamport hiện tại của mình
        XeTransaction ack = new XeTransaction();
        ack.jobId        = tx.jobId;
        ack.bienSo       = tx.bienSo;
        ack.originGateId = tx.originGateId;
        ack.lampClock    = lampClock.incrementAndGet(); // Tăng trước khi gửi
        ack.phase        = "ACK";
        ack.fromGate     = myGateId;
        ack.wallTime     = System.currentTimeMillis();

        log(String.format("✅ [GỬI ACK|L:%d] Xác nhận jobId=%s về Cổng %s",
                ack.lampClock, tx.jobId, tx.originGateId));

        return gson.toJson(ack);
    }

    // ════════════════════════════════════════════════════════════
    //  PHA 3 - COMMIT: Khi nhận đủ ACK → phát COMMIT cho tất cả
    // ════════════════════════════════════════════════════════════

    /**
     * GỌI KHI: Server khởi tạo nhận được một ACK từ peer.
     * HÀNH ĐỘNG: Nếu đủ ACK → broadcast COMMIT đến tất cả server
     */
    public void nhanAck(XeTransaction ackTx) {
        String jobId = ackTx.jobId;

        // Cập nhật đồng hồ Lamport
        long newClock = Math.max(lampClock.get(), ackTx.lampClock) + 1;
        lampClock.set(newClock);

        Set<String> acks = ackMap.get(jobId);
        if (acks == null) {
            // Không phải giao dịch của mình → bỏ qua
            return;
        }

        acks.add(ackTx.fromGate);
        log(String.format("📬 [NHẬN ACK] từ Cổng %s cho %s (%d/%d ACK)",
                ackTx.fromGate, jobId, acks.size(), peerUrls.size()));

        // Kiểm tra: đã nhận đủ ACK từ tất cả peer chưa?
        if (acks.size() >= peerUrls.size()) {
            log(String.format("🎯 [ĐỦ ACK!] jobId=%s → Broadcast COMMIT cho tất cả!", jobId));
            broadcastCommit(jobId);
        }
    }

    /**
     * Phát thông điệp COMMIT đến tất cả server (kể cả chính mình)
     */
    private void broadcastCommit(String jobId) {
        // Tự xử lý commit cho chính mình
        xuLyCommit(jobId);

        // Gửi commit đến các peer
        for (String peerUrl : peerUrls) {
            final String url = peerUrl;
            CompletableFuture.runAsync(() -> {
                guiHttp(url + "/api/p2p/commit", jobId, "COMMIT→" + url);
            });
        }
    }

    // ════════════════════════════════════════════════════════════
    //  PHA 4 - WRITE: Nhận COMMIT → Ghi vào CSDL theo thứ tự Lamport
    // ════════════════════════════════════════════════════════════

    /**
     * GỌI KHI: Nhận được thông điệp COMMIT (từ peer hoặc từ chính mình).
     * HÀNH ĐỘNG: Thêm vào hàng đợi COMMIT → Xử lý ghi CSDL theo thứ tự
     */
    public synchronized void xuLyCommit(String jobId) {
        XeTransaction tx = pendingMap.get(jobId);
        if (tx == null) {
            log("⚠️ [COMMIT] Không tìm thấy tx jobId=" + jobId + " trong pendingMap");
            return;
        }

        if (writtenJobIds.contains(jobId)) {
            return; // Đã ghi rồi, bỏ qua
        }

        // Thêm vào hàng đợi commit (tự động sắp xếp theo Lamport clock)
        committedQueue.add(tx);

        log(String.format("📋 [ĐẶT VÀO HÀNG ĐỢI|L:%d] %s - Hàng đợi có %d giao dịch",
                tx.lampClock, tx.bienSo, committedQueue.size()));

        // Ghi tất cả giao dịch trong hàng đợi vào CSDL theo thứ tự Lamport
        ghiCSDLTheoThuTuLamport();
    }

    /**
     * ================================================================
     *  GHI VÀO CSDL THEO THỨ TỰ LAMPORT - Đây là bước quyết định!
     * ================================================================
     *  TreeSet<XeTransaction> tự sắp xếp theo compareTo():
     *    Ưu tiên 1: lampClock nhỏ → xảy ra trước
     *    Ưu tiên 2: originGateId nhỏ → tie-break
     *
     *  Vì TẤT CẢ Server dùng CÙNG THỨ TỰ SẮP XẾP NÀY:
     *  → global_seq ở 3 Server sẽ KHỚP NHAU 100%!
     * ================================================================
     */
    private void ghiCSDLTheoThuTuLamport() {
        // Duyệt hàng đợi theo đúng thứ tự Lamport (TreeSet đã sắp xếp sẵn)
        List<XeTransaction> danhSachGhi = new ArrayList<>(committedQueue);

        for (XeTransaction tx : danhSachGhi) {
            if (writtenJobIds.contains(tx.jobId)) continue;

            try {
                long seq = nextGlobalSeq;

                // ⭐ GHI VÀO CSDL CHÍNH - Đây là bước cuối cùng!
                db.insertXeLog(
                    seq,
                    tx.jobId,
                    tx.bienSo,
                    tx.originGateId,
                    tx.lampClock,
                    "Vao bai",
                    myGateId  // Cổng nào ghi
                );

                // Dọn dẹp bảng tạm
                db.deletePending(tx.jobId);

                // Đánh dấu đã ghi
                writtenJobIds.add(tx.jobId);
                nextGlobalSeq++;

                // Ghi log lịch sử
                String thongBao = String.format(
                    "✅ [SEQ:%d|L:%d] Xe %s vào bãi (Khởi từ Cổng %s, ghi bởi Cổng %s)",
                    seq, tx.lampClock, tx.bienSo, tx.originGateId, myGateId
                );
                lichSu.add(0, thongBao);
                if (lichSu.size() > 50) lichSu.remove(50);

                System.out.printf("[CSDL] Đã ghi SEQ:%d | L:%d | Xe %s | Cổng %s%n",
                    seq, tx.lampClock, tx.bienSo, tx.originGateId);

            } catch (Exception e) {
                System.err.println("[DB ERROR] Lỗi ghi CSDL: " + e.getMessage());
            }
        }

        // Dọn dẹp bộ nhớ
        committedQueue.removeIf(tx -> writtenJobIds.contains(tx.jobId));
        pendingMap.entrySet().removeIf(e -> writtenJobIds.contains(e.getKey()));
    }

    // ════════════════════════════════════════════════════════════
    //  THÔNG TIN TRẠNG THÁI (để hiển thị lên giao diện)
    // ════════════════════════════════════════════════════════════

    public long getLampClock() { return lampClock.get(); }

    public long getNextGlobalSeq() { return nextGlobalSeq; }

    public List<XeTransaction> getPendingList() {
        return new ArrayList<>(pendingMap.values());
    }

    public List<XeTransaction> getCommittedQueueSnapshot() {
        synchronized(this) {
            return new ArrayList<>(committedQueue);
        }
    }

    // ════════════════════════════════════════════════════════════
    //  HÀM TIỆN ÍCH
    // ════════════════════════════════════════════════════════════

    private void savePendingToDB(XeTransaction tx) {
        try {
            db.insertPending(tx.jobId, tx.bienSo, tx.originGateId,
                             tx.lampClock, tx.phase, tx.wallTime);
        } catch (Exception e) {
            System.err.println("[DB WARN] Không lưu được pending: " + e.getMessage());
        }
    }

    private void guiHttp(String url, String body, String label) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            System.out.printf("[P2P] %s → HTTP %d%n", label, response.statusCode());
        } catch (Exception e) {
            System.err.printf("[P2P WARN] %s thất bại: %s%n", label, e.getMessage());
        }
    }

    private void log(String msg) {
        System.out.println("[Cổng " + myGateId + "] " + msg);
    }
}