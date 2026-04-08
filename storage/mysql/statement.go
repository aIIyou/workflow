package mysql

const (
	CreateEvent = "INSERT INTO `event_queue`(`event_id`,`type`,`name`,`status`,`flow_id`,`flow_type`)VALUES(?,?,?,?,?,?)"

	RetrievePendingEvent = "SELECT * FROM `event_queue` WHERE `status` = 'pending' AND `heartbeat_at` IS NULL LIMIT 1 FOR UPDATE"

	RetrieveFlowPendingEvent = "SELECT * FROM `event_queue` WHERE `flow_id` = ? AND `status` = 'pending' AND `visible_at` <= now()  LIMIT 1 FOR UPDATE"

	UpdatePendingEventStatus = "UPDATE `event_queue` set `status` = 'processing' WHERE `flow_id` = ?"

	UpdateEventHeartbeat = "UPDATE `event_queue` SET `heartbeat_at` = NOW() WHERE `event_id` = ?"

	RetrieveEventFlow = "SELECT * From `event_flow` WHERE `flow_id` = ?"
)

const (
	create_flow_table = `
CREATE TABLE work_flow(
    flow_id varchar(256),
    flow_type varchar(256),
    flow_name varchar(256),
    current_event varchar(256)
)`
)
