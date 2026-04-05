package com.example.model;

/**
 * ================================================================
 *  GIAO DỊCH XE - CÓ NHÃN ĐỒNG HỒ LAMPORT
 * ================================================================
 *  Mỗi lần xe vào bãi tạo ra 1 XeTransaction.
 *  Đồng hồ Lamport (lampClock) đảm bảo:
 *    - Các Server thống nhất thứ tự các giao dịch
 *    - Dữ liệu ghi vào CSDL theo đúng 1 thứ tự duy nhất
 *
 *  Cấu trúc thông điệp (theo slide thầy Vĩ - Hình 4):
 *  @$ | originGate | lampClock | jeton | phase | fromGate | $$ | bienSo | @$
 * ================================================================
 */
public class XeTransaction implements Comparable<XeTransaction> {

    // ─── Thông tin định danh ─────────────────────────────────────
    /** ID duy nhất của giao dịch (UUID) */
    public String jobId;

    /** Biển số xe */
    public String bienSo;

    /** Cổng KHỞI TẠO giao dịch này (1, 2 hoặc 3) */
    public String originGateId;

    // ─── Đồng hồ Lamport ────────────────────────────────────────
    /**
     * Đồng hồ logic Lamport - trái tim của thuật toán đồng thuận.
     * Quy tắc cập nhật:
     *   Khi GỬI:    clock = ++localClock
     *   Khi NHẬN:   clock = max(localClock, receivedClock) + 1
     */
    public long lampClock;

    // ─── Pha giao dịch (4 vòng theo slide thầy) ─────────────────
    /**
     * Pha của thông điệp trong vòng tròn ảo:
     *   PROPOSE  → Pha 1: Đề xuất + gắn đồng hồ Lamport
     *   ACK      → Pha 2: Xác nhận đã nhận và cập nhật bảng tạm
     *   COMMIT   → Pha 3: Đồng thuận xong, ghi vào CSDL chính
     */
    public String phase;

    /** Cổng đang gửi thông điệp này (cập nhật khi di chuyển trong hệ) */
    public String fromGate;

    // ─── Thứ tự toàn cục sau đồng thuận ─────────────────────────
    /**
     * Số thứ tự toàn cục (global sequence) sau khi tất cả Server đồng thuận.
     * Đây là thứ tự ghi vào CSDL - 3 Server phải có cùng số này!
     */
    public long globalSeq;

    /** Thời điểm thực tạo giao dịch (để hiển thị, không dùng để sắp xếp) */
    public long wallTime;

    // ─── Constructors ────────────────────────────────────────────
    public XeTransaction() {}

    public XeTransaction(String jobId, String bienSo, String originGateId, long lampClock) {
        this.jobId       = jobId;
        this.bienSo      = bienSo;
        this.originGateId = originGateId;
        this.lampClock   = lampClock;
        this.phase       = "PROPOSE";
        this.fromGate    = originGateId;
        this.globalSeq   = 0;
        this.wallTime    = System.currentTimeMillis();
    }

    /**
     * ================================================================
     *  SẮP XẾP THEO ĐỒNG HỒ LAMPORT
     * ================================================================
     *  Đây là trái tim của thuật toán!
     *  Khi tất cả Server sắp xếp các giao dịch theo compareTo() này,
     *  họ sẽ ra CÙNG MỘT THỨ TỰ DUY NHẤT.
     *
     *  Ưu tiên 1: lampClock nhỏ hơn → xảy ra trước
     *  Ưu tiên 2 (tie-break): originGateId nhỏ hơn (để chống đụng độ)
     * ================================================================
     */
    @Override
    public int compareTo(XeTransaction other) {
        if (this.lampClock != other.lampClock) {
            return Long.compare(this.lampClock, other.lampClock);
        }
        // Tie-break: cổng có ID nhỏ hơn được ưu tiên
        return this.originGateId.compareTo(other.originGateId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof XeTransaction)) return false;
        return this.jobId.equals(((XeTransaction) obj).jobId);
    }

    @Override
    public int hashCode() {
        return jobId.hashCode();
    }

    @Override
    public String toString() {
        return String.format("[L:%d|Cong:%s|%s|%s]", lampClock, originGateId, bienSo, phase);
    }
}