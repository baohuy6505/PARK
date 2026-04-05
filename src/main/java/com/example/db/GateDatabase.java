package com.example.db;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;

/**
 * ================================================================
 *  TẦNG CƠ SỞ DỮ LIỆU (DATABASE LAYER)
 * ================================================================
 *  Quản lý 2 bảng chính theo kiến trúc của thầy:
 *
 *  1. pending_queue  → Bảng tạm (Vòng 2 - TEMP)
 *     Lưu các giao dịch đang chờ đồng thuận Lamport
 *
 *  2. xe_log         → Bảng chính (Vòng 4 - UPDATE)
 *     Lưu giao dịch SAU KHI đồng thuận thành công
 *     Đây là bảng mà 3 Server phải có dữ liệu KHỚP NHAU
 * ================================================================
 */
public class GateDatabase {

    private final HikariDataSource ds;

    public GateDatabase(HikariDataSource ds) {
        this.ds = ds;
    }

    /**
     * Tạo schema database khi khởi động
     */
    public void runSchema() throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {

            // Bảng tạm - chứa giao dịch đang chờ ĐỒNG THUẬN
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS pending_queue (
                    job_id      VARCHAR(50)  PRIMARY KEY,
                    bien_so     VARCHAR(30)  NOT NULL,
                    origin_gate VARCHAR(10)  NOT NULL,
                    lamp_clock  BIGINT       NOT NULL,
                    phase       VARCHAR(20)  NOT NULL DEFAULT 'PROPOSE',
                    wall_time   BIGINT       NOT NULL,
                    created_at  TIMESTAMP    DEFAULT NOW()
                )
            """);

            // Bảng chính - chứa giao dịch ĐÃ ĐỒNG THUẬN xong
            // Tất cả 3 Server PHẢI CÓ CÙNG DỮ LIỆU trong bảng này!
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS xe_log (
                    global_seq  BIGINT       PRIMARY KEY,
                    job_id      VARCHAR(50)  NOT NULL,
                    bien_so     VARCHAR(30)  NOT NULL,
                    origin_gate VARCHAR(10)  NOT NULL,
                    lamp_clock  BIGINT       NOT NULL,
                    trang_thai  VARCHAR(30)  NOT NULL,
                    ghi_boi     VARCHAR(10)  NOT NULL,
                    created_at  TIMESTAMP    DEFAULT NOW()
                )
            """);

            System.out.println("[DB] Schema khởi tạo xong (pending_queue + xe_log)");
        }
    }

    // ════════════════════════════════════════════════
    //  BẢNG TẠM - pending_queue
    // ════════════════════════════════════════════════

    /** Lưu giao dịch vào bảng tạm khi nhận PROPOSE */
    public void insertPending(String jobId, String bienSo, String originGate,
                              long lampClock, String phase, long wallTime) throws SQLException {
        String sql = """
            INSERT IGNORE INTO pending_queue (job_id, bien_so, origin_gate, lamp_clock, phase, wall_time)
            VALUES (?, ?, ?, ?, ?, ?)
        """;
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, jobId);
            ps.setString(2, bienSo);
            ps.setString(3, originGate);
            ps.setLong(4, lampClock);
            ps.setString(5, phase);
            ps.setLong(6, wallTime);
            ps.executeUpdate();
        }
    }

    /** Xóa giao dịch khỏi bảng tạm khi đã commit vào bảng chính */
    public void deletePending(String jobId) throws SQLException {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                 "DELETE FROM pending_queue WHERE job_id = ?")) {
            ps.setString(1, jobId);
            ps.executeUpdate();
        }
    }

    // ════════════════════════════════════════════════
    //  BẢNG CHÍNH - xe_log (SAU KHI ĐỒNG THUẬN)
    // ════════════════════════════════════════════════

    /**
     * Ghi giao dịch vào bảng CHÍNH sau khi đồng thuận Lamport thành công.
     * global_seq = thứ tự ĐỒNG THUẬN → 3 Server phải khớp nhau!
     */
    public void insertXeLog(long globalSeq, String jobId, String bienSo,
                            String originGate, long lampClock,
                            String trangThai, String ghiBoi) throws SQLException {
        String sql = """
            INSERT INTO xe_log (global_seq, job_id, bien_so, origin_gate, lamp_clock, trang_thai, ghi_boi)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """;
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, globalSeq);
            ps.setString(2, jobId);
            ps.setString(3, bienSo);
            ps.setString(4, originGate);
            ps.setLong(5, lampClock);
            ps.setString(6, trangThai);
            ps.setString(7, ghiBoi);
            ps.executeUpdate();
        }
    }

    /** Lấy giá trị đồng hồ Lamport lớn nhất để khôi phục khi restart */
    public long getMaxLampClock() throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COALESCE(MAX(lamp_clock), 0) FROM xe_log")) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    /** Lấy global_seq lớn nhất để biết sequence tiếp theo khi restart */
    public long getMaxGlobalSeq() throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COALESCE(MAX(global_seq), 0) FROM xe_log")) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    /**
     * Lấy toàn bộ lịch sử xe_log để hiển thị trên giao diện web
     * Sắp xếp theo global_seq → đúng thứ tự đồng thuận
     */
    public java.util.List<java.util.Map<String, Object>> getXeLog(int limit) throws SQLException {
        var result = new java.util.ArrayList<java.util.Map<String, Object>>();
        String sql = """
            SELECT global_seq, bien_so, origin_gate, lamp_clock, trang_thai, ghi_boi, created_at
            FROM xe_log
            ORDER BY global_seq DESC
            LIMIT ?
        """;
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                var row = new java.util.LinkedHashMap<String, Object>();
                row.put("seq",       rs.getLong("global_seq"));
                row.put("bienSo",    rs.getString("bien_so"));
                row.put("gate",      rs.getString("origin_gate"));
                row.put("clock",     rs.getLong("lamp_clock"));
                row.put("trangThai", rs.getString("trang_thai"));
                row.put("ghiBoi",    rs.getString("ghi_boi"));
                row.put("time",      rs.getTimestamp("created_at").toString());
                result.add(row);
            }
        }
        return result;
    }
}