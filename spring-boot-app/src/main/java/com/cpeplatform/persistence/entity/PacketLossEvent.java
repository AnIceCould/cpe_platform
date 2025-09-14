package com.cpeplatform.persistence.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JPA实体类，映射到数据库中的 'packet_loss_events' 表。
 * 用于记录每一次检测到的丢包事件。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity // 声明这是一个JPA实体
@Table(name = "packet_loss_events") // 指定数据库中的表名
public class PacketLossEvent {

    @Id // 声明这是主键
    @GeneratedValue(strategy = GenerationType.IDENTITY) // 设置主键为自增
    private Long id;

    @Column(name = "device_id", nullable = false) // 映射到 'device_id' 列，且不能为空
    private String deviceId;

    @Column(name = "event_timestamp", nullable = false) // 映射到 'event_timestamp' 列
    private long eventTimestamp;

    @Column(name = "has_packet_loss", nullable = false)
    private boolean hasPacketLoss;
}
