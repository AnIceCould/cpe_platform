package com.cpeplatform.adapter.websocket;

import com.cpeplatform.dto.PredictionResultDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 负责处理 WebSocket 连接和消息广播。
 */
@Component
public class AlertWebsocketHandler extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(AlertWebsocketHandler.class);

    // 使用线程安全的列表来存储所有活跃的 WebSocket session
    private final List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 当一个新的 WebSocket 连接建立时被调用。
     */
    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        sessions.add(session);
        logger.info("✅ 新的前端连接已建立: {}", session.getId());
    }

    /**
     * 当一个 WebSocket 连接关闭时被调用。
     */
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        sessions.remove(session);
        logger.info("🔌 一个前端连接已关闭: {}", session.getId());
    }

    /**
     * 【核心广播方法】
     * 将丢包事件发送给所有连接的前端客户端。
     * @param resultDto 预测结果
     */
    public void sendPacketLossAlert(PredictionResultDto resultDto) {
        try {
            // 将Java对象转换为JSON字符串
            String alertMessage = objectMapper.writeValueAsString(resultDto);
            TextMessage message = new TextMessage(alertMessage);

            logger.info("📡 准备向 {} 个前端连接广播丢包警报...", sessions.size());

            // 遍历所有 session 并发送消息
            for (WebSocketSession session : sessions) {
                if (session.isOpen()) {
                    session.sendMessage(message);
                }
            }
        } catch (IOException e) {
            logger.error("❌ 广播 WebSocket 消息时出错", e);
        }
    }
}
