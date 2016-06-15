create database if not exists lobster;
use lobster;

SET GLOBAL event_scheduler = 1;

--
--
-- table of updates
--
-- ver - last version
-- topic - topic of updating
-- 		0: metric
-- id - topic id, e.g metric id if topic == 0
-- op - operation
--		0 : create
--		1 : modify
--		2 : remove
-- ts - update timestamp
--
create table if not exists t_updates (
	`ver` bigint unsigned auto_increment not null,
	`topic` tinyint unsigned not null,
	`id` int unsigned not null,
	`op` tinyint unsigned default 0 not null,
	`ts` timestamp default current_timestamp,
	primary key (`ver`)
) engine=InnoDB default charset=utf8;

--
--
-- create event schedule for clean t_updates
--
drop event if exists event_clean_update;
create event if not exists event_clean_update
  on schedule every 1 day
  do delete from t_updates where ts < date_sub(now(), interval 1 hour);

--
-- 
-- do update procedure
--
drop procedure if exists update_topic_op;

delimiter $

create procedure update_topic_op(
	topic_ tinyint unsigned,    # topic type
	id_ int unsigned,           # topic id
	op_ tinyint unsigned        # op
)
begin
	insert into t_updates (topic, id, op) values (topic_, id_, op_);
end$

delimiter ;

--
--
-- table of metrics
--
-- id - metric id, pk
-- name - metric name, unique
-- ver - version, increment from 0
-- keys - json string
-- create_time - timestamp
-- modify_time - last modify time, timestamp
--
create table if not exists t_metrics (
	`id` int unsigned auto_increment not null,
	`name` varchar(32) not null,
	`desc` varchar(64),
	`ver` int unsigned default 0,
	`keys` text,
	`create_time` timestamp default current_timestamp,
	`modify_time` timestamp,
	primary key (`id`),
	unique key `t_metrics_uidx_1` (`name`) 
) engine=InnoDB default charset=utf8;

delimiter $

drop trigger if exists t_metrics_before_insert;
create trigger t_metrics_before_insert before insert on t_metrics
	for each row begin
		set new.modify_time = now();
	end $

drop trigger if exists t_metrics_after_insert;
create trigger t_metrics_after_insert after insert on t_metrics
	for each row begin
		call update_topic_op(0, new.id, 0);
	end $

drop trigger if exists t_metrics_before_update;
create trigger t_metrics_before_update before update on t_metrics
	for each row begin
		set new.modify_time = now();
		set new.ver = new.ver + 1;
	end $

drop trigger if exists t_metrics_after_update;
create trigger t_metrics_after_update after update on t_metrics
	for each row begin
		call update_topic_op(0, new.id, 1);
	end $

drop trigger if exists t_metrics_after_delete;
create trigger t_metrics_after_delete after delete on t_metrics
	for each row begin
		call update_topic_op(0, old.id, 2);
	end $

delimiter ;