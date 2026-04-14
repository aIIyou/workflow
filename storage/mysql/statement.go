package mysql

const (
	CreateEvent = "INSERT INTO `event_queue`(`event_id`,`type`,`async`,`name`,`status`,`flow_id`,`flow_type`,`flow_name`,`visible_at`)VALUES(?,?,?,?,?,?,?,?,?)"

	RetrievePendingEvent = "SELECT * FROM `event_queue` WHERE `status` = 'pending' AND `async` = true LIMIT 1 FOR UPDATE"

	RetrieveFlowPendingEvent = "SELECT * FROM `event_queue` WHERE `flow_id` = ? AND `status` = 'pending' AND `visible_at` <= now()  LIMIT 1 FOR UPDATE"

	UpdateEventStatus = "UPDATE `event_queue` set `status` = ? WHERE `event_id` = ?"

	UpdatePendingEventStatus = "UPDATE `event_queue` set `status` = 'processing' WHERE `flow_id` = ?"

	UpdateEventHeartbeat = "UPDATE `event_queue` SET `heartbeat_at` = NOW() WHERE `event_id` = ?"

	CreateEventFlow = "INSERT INTO `event_flow`(`flow_id`,`name`,`type`,`data`,`status`,`current_event_name`)VALUES(?,?,?,?,?,?)"

	RetrieveEventFlow = "SELECT * From `event_flow` WHERE `flow_id` = ?"

	UpdateEventFlowData = "UPDATE `event_flow` SET `data` = ? WHERE `flow_id` = ?"

	RetrieveFlowCurrentEvent = "SELECT * FROM `event_queue` WHERE `flow_id` = ? ORDER BY `created_at` DESC LIMIT 1"

	UpdateEventVisibleAt = "UPDATE `event_queue` SET `visible_at` = ? WHERE `event_id` = ?"
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
