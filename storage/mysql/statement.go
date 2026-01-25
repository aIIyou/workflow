package mysql

const (
	CreateEvent = "INSERT INTO `event_queue`(`event_id`,`type`,`name`,`status`,`flow_id`,`flow_type`)VALUES(?,?,?,?,?,?)"

	RetrievePendingEvent = "SELECT * FROM `event_queue` WHERE `status` = 'pending' AND `heartbeat_at` IS NULL LIMIT 1 FOR UPDATE"
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
