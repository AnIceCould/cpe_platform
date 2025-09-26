package com.cpeplatform.persistence.repository;

import com.cpeplatform.persistence.entity.DeviceStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface DeviceStatusRepository extends JpaRepository<DeviceStatus, Long> {

    Optional<DeviceStatus> findByDeviceId(String deviceId);

    /**
     * 使用原生的SQL查询来实现高效的 UPSERT 操作。
     *
     * @param deviceId 设备ID
     * @param status   新状态
     * @param timestamp 最后更新时间戳
     */
    @Modifying // 声明这是一个修改数据库状态的操作
    @Transactional // 确保这个操作在它自己的事务中执行
    @Query(
            value = "INSERT INTO device_statuses (device_id, status, last_updated) " +
                    "VALUES (:deviceId, :status, :timestamp) " +
                    "ON DUPLICATE KEY UPDATE status = :status, last_updated = :timestamp",
            nativeQuery = true // 声明这是一个原生SQL查询
    )
    void upsertStatus(@Param("deviceId") String deviceId,
                      @Param("status") String status,
                      @Param("timestamp") long timestamp);
}

