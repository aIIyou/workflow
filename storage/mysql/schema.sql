-- event_flow: table
CREATE TABLE `event_flow` (
                              `id` bigint NOT NULL AUTO_INCREMENT,
                              `flow_id` varchar(255) NOT NULL,
                              `name` varchar(255) NOT NULL,
                              `type` varchar(255) DEFAULT NULL,
                              `data` longtext,
                              `status` varchar(128) DEFAULT NULL,
                              `current_event_name` varchar(255) DEFAULT NULL,
                              `create_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                              `update_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                              PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- event_queue: table
CREATE TABLE `event_queue` (
                               `id` bigint NOT NULL AUTO_INCREMENT,
                               `event_id` varchar(255) NOT NULL COMMENT '事件标识',
                               `name` varchar(255) NOT NULL COMMENT '事件名称',
                               `type` varchar(255) DEFAULT NULL COMMENT '事件类型',
                               `async` bit(1) NOT NULL DEFAULT b'0' COMMENT '0:事件被同步处理;1:事件被异步处理',
                               `status` varchar(255) NOT NULL COMMENT '事件状态',
                               `flow_id` varchar(255) NOT NULL COMMENT '工作流标识',
                               `flow_type` varchar(255) NOT NULL COMMENT '工作流类型',
                               `flow_name` varchar(255) NOT NULL,
                               `create_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间戳',
                               `update_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间戳',
                               `heartbeat_at` timestamp NULL DEFAULT NULL COMMENT '心跳时间戳',
                               `visible_at` timestamp NULL DEFAULT NULL COMMENT '事件可见时间',
                               `worker_ip` varchar(255) DEFAULT NULL,
                               `worker_id` varchar(255) DEFAULT NULL,
                               PRIMARY KEY (`id`),
                               KEY `idx_flow_id` (`flow_id`),
                               KEY `idx_event_name` (`name`),
                               KEY `idx_status` (`status`),
                               KEY `idx_status_async_heart_beat_at` (`status`,`async`,`heartbeat_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- No native definition for element: idx_event_name (index)

-- No native definition for element: idx_status_async_heart_beat_at (index)

-- No native definition for element: idx_status (index)

-- No native definition for element: idx_flow_id (index)


CREATE TABLE `event_retention` (
                                   `id` bigint NOT NULL AUTO_INCREMENT,
                                   `event_id` varchar(255) NOT NULL,
                                   `event_name` varchar(255) NOT NULL,
                                   `milestone` varchar(255) NOT NULL,
                                   `create_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP /*!80023 INVISIBLE */,
                                   `update_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   PRIMARY KEY (`id`),
                                   UNIQUE KEY `idx_event_id_milestone` (`event_id`,`milestone`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;





