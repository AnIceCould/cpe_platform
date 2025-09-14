package com.cpeplatform.persistence.repository;

import com.cpeplatform.persistence.entity.PacketLossEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA 仓库接口，用于操作 PacketLossEvent 实体。
 * JpaRepository<PacketLossEvent, Long> 中的 Long 指的是主键 id 的类型。
 */
@Repository
public interface PacketLossEventRepository extends JpaRepository<PacketLossEvent, Long> {
    // Spring Data JPA 会自动提供 save(), findById(), findAll() 等方法。
    // 如果需要自定义查询，可以在这里定义方法，例如：
    // List<PacketLossEvent> findByDeviceId(String deviceId);
}
