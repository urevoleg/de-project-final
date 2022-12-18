CREATE TABLE IF NOT EXISTS UREVOLEGYANDEXRU__STAGING.srv_wf_settings (
	id AUTO_INCREMENT,
	source varchar NOT NULL,
	settings varchar NOT NULL,
	loaded_at timestamp NOT NULL
)
order by loaded_at
SEGMENTED BY hash(loaded_at::date) all nodes
PARTITION BY loaded_at::date
GROUP BY calendar_hierarchy_day(loaded_at::date, 3, 2);