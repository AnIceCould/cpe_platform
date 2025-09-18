package com.cpeplatform.config;

import com.cpeplatform.adapter.websocket.AlertWebsocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * 启用并配置 WebSocket。
 */
@Configuration
@EnableWebSocket // 开启 WebSocket 支持
public class WebSocketConfig implements WebSocketConfigurer {

    private final AlertWebsocketHandler alertWebsocketHandler;

    public WebSocketConfig(AlertWebsocketHandler alertWebsocketHandler) {
        this.alertWebsocketHandler = alertWebsocketHandler;
    }

    /**
     * 注册 WebSocket 处理器。
     * @param registry 处理器注册器
     */
    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(alertWebsocketHandler, "/ws/alerts")
                .setAllowedOrigins("*"); // 允许所有来源的跨域连接
    }
}
