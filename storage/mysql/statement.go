package mysql

const (
	CreateEvent = "INSERT INTO event_queue(`event_id`,`type`,`name`,`status`,`flow_id`,`flow_type`)VALUES(?,?,?,?,?,?)"
)
