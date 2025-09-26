package com.cpeplatform.persistence.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JPA实体类，映射到数据库中的 'device_statuses' 表。
 * 用于存储每个CPE设备的最新状态。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "device_statuses")
public class DeviceStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // 设置 deviceId 为唯一，确保每个设备只有一条状态记录
    @Column(name = "device_id", nullable = false, unique = true)
    private String deviceId;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "last_updated", nullable = false)
    private long lastUpdated; // 记录状态最后更新的时间戳
}
