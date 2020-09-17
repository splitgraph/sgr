--
-- Build the Matomo view in matomo schema (pointing to data in matomo_raw)
--


DROP SCHEMA IF EXISTS matomo CASCADE;
CREATE SCHEMA matomo;

--
-- Helper function to pretty print IP addresses
--
CREATE OR REPLACE FUNCTION matomo.bytea_to_ip(
    ip_address bytea
) RETURNS text AS $$
    SELECT
        get_byte(ip_address, 0)::text || '.'
        || get_byte(ip_address, 1)::text || '.'
        || get_byte(ip_address, 2)::text || '.'
        || get_byte(ip_address, 3)::text
$$ LANGUAGE SQL;

--
-- User-readable view to Matomo's visit table
--
CREATE OR REPLACE VIEW matomo.visit AS (
    SELECT
        idvisit AS visit_id,
        encode(idvisitor, 'hex') AS visitor_id,
        user_id,
        matomo.bytea_to_ip(location_ip) AS remote_address,
        visit_first_action_time,
        visit_last_action_time,
        la_entry_name.name AS visit_entry_name,
        la_entry_url.name AS visit_entry_url,
        la_exit_name.name AS visit_exit_name,
        la_exit_url.name AS visit_exit_url,
        visit_total_actions,
        visit_total_interactions,
        visit_total_searches,
        visit_total_time,
        visit_total_events,

        visitor_days_since_first,
        visitor_days_since_last,
        visitor_returning,
        visitor_count_visits
    FROM
        matomo_raw.matomo_log_visit lv
    LEFT OUTER JOIN matomo_raw.matomo_log_action la_entry_name ON lv.visit_entry_idaction_name = la_entry_name.idaction
    LEFT OUTER JOIN matomo_raw.matomo_log_action la_exit_name ON lv.visit_exit_idaction_name = la_exit_name.idaction
    LEFT OUTER JOIN matomo_raw.matomo_log_action la_entry_url ON lv.visit_entry_idaction_url = la_entry_url.idaction
    LEFT OUTER JOIN matomo_raw.matomo_log_action la_exit_url ON lv.visit_exit_idaction_url = la_exit_url.idaction
);
