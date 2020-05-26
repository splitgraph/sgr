-- From https://github.com/2ndQuadrant/audit-trigger/ with a couple of patches applied:
-- * https://github.com/2ndQuadrant/audit-trigger/pull/21 to remove quote_ident
--   (otherwise can't audit tables that are schema-qualified or with non-lowercase names).
-- * https://github.com/2ndQuadrant/audit-trigger/pull/32 to change hstore storage
--   to jsonb (jsonb didn't exist when the trigger was made + hstore seems to discard
--   column type information)
-- * Not storing unused tx information in the audit table (conn details, users etc)
-- * Not allowing to exclude columns
-- At first I set it up to fetch master from github and apply the patches but the master
-- hasn't been updated for 3 years and these PRs have no chance of getting merged.
-- An audit history is important on most tables. Provide an audit trigger that logs to
-- a dedicated audit table for the major relations.
--
-- This file should be generic and not depend on application roles or structures,
-- as it's being listed here:
--
--    https://wiki.postgresql.org/wiki/Audit_trigger_91plus
--
-- This trigger was originally based on
--   http://wiki.postgresql.org/wiki/Audit_trigger
-- but has been completely rewritten.
--
-- Should really be converted into a relocatable EXTENSION, with control and upgrade files.
CREATE SCHEMA splitgraph_audit;

REVOKE ALL ON SCHEMA splitgraph_audit FROM public;

COMMENT ON SCHEMA splitgraph_audit IS 'Out-of-table audit/history logging tables and trigger functions';

--
-- Audited data. Lots of information is available, it's just a matter of how much
-- you really want to record. See:
--
--   http://www.postgresql.org/docs/9.1/static/functions-info.html
--
-- Remember, every column you add takes up more audit table space and slows audit
-- inserts.
--
-- Every index you add has a big impact too, so avoid adding indexes to the
-- audit table unless you REALLY need them. The json GIN/GIST indexes are
-- particularly expensive.
--
-- It is sometimes worth copying the audit table, or a coarse subset of it that
-- you're interested in, into a temporary table where you CREATE any useful
-- indexes and do your analysis.
--
CREATE TABLE splitgraph_audit.logged_actions (
    event_id bigserial PRIMARY KEY,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    action text NOT NULL CHECK (action IN ('I', 'D', 'U', 'T')),
    row_data jsonb,
    changed_fields jsonb
);

REVOKE ALL ON splitgraph_audit.logged_actions FROM public;

COMMENT ON TABLE splitgraph_audit.logged_actions IS 'History of auditable actions on audited tables, from audit.if_modified_func()';

COMMENT ON COLUMN splitgraph_audit.logged_actions.event_id IS 'Unique identifier for each auditable event';

COMMENT ON COLUMN splitgraph_audit.logged_actions.schema_name IS 'Database schema audited table for this event is in';

COMMENT ON COLUMN splitgraph_audit.logged_actions.table_name IS 'Non-schema-qualified table name of table event occured in';

COMMENT ON COLUMN splitgraph_audit.logged_actions.action IS 'Action type; I = insert, D = delete, U = update, T = truncate';

COMMENT ON COLUMN splitgraph_audit.logged_actions.row_data IS 'Record value. Null for statement-level trigger. For INSERT this is the new tuple. For DELETE and UPDATE it is the old tuple.';

COMMENT ON COLUMN splitgraph_audit.logged_actions.changed_fields IS 'New values of fields changed by UPDATE. Null except for row-level UPDATE events.';

CREATE INDEX logged_actions_action_idx ON splitgraph_audit.logged_actions (action);

CREATE OR REPLACE FUNCTION splitgraph_audit.if_modified_func ()
    RETURNS TRIGGER
    AS $body$
DECLARE
    audit_row splitgraph_audit.logged_actions;
    h_old jsonb;
    h_new jsonb;
BEGIN
    IF TG_WHEN <> 'AFTER' THEN
        RAISE EXCEPTION 'splitgraph_audit.if_modified_func() may only run as an AFTER trigger';
    END IF;
    audit_row = ROW (nextval('splitgraph_audit.logged_actions_event_id_seq'), -- event_id
        TG_TABLE_SCHEMA::text, -- schema_name
        TG_TABLE_NAME::text, -- table_name
        substring(TG_OP, 1, 1), -- action
        NULL,
        NULL -- row_data, changed_fields
);
    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
        audit_row.row_data = row_to_json(OLD)::jsonb;
        --Computing differences
	SELECT jsonb_object_agg(tmp_new_row.key, tmp_new_row.value) AS new_data
	    INTO audit_row.changed_fields
        FROM jsonb_each_text(row_to_json(NEW)::jsonb) AS tmp_new_row
    JOIN jsonb_each_text(audit_row.row_data) AS tmp_old_row ON (tmp_new_row.key
	= tmp_old_row.key
            AND tmp_new_row.value IS DISTINCT FROM tmp_old_row.value);
        IF audit_row.changed_fields = '{}'::jsonb THEN
            -- All changed fields are ignored. Skip this update.
            RETURN NULL;
        END IF;
    ELSIF (TG_OP = 'DELETE'
            AND TG_LEVEL = 'ROW') THEN
        audit_row.row_data = row_to_json(OLD)::jsonb;
    ELSIF (TG_OP = 'INSERT'
            AND TG_LEVEL = 'ROW') THEN
        audit_row.row_data = row_to_json(NEW)::jsonb;
    ELSE
        RAISE EXCEPTION '[splitgraph_audit.if_modified_func] - Trigger func added as
     	    trigger for unhandled case: %, %', TG_OP, TG_LEVEL;
        RETURN NULL;
    END IF;
    INSERT INTO splitgraph_audit.logged_actions
        VALUES (audit_row.*);
    RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = pg_catalog, public;

COMMENT ON FUNCTION splitgraph_audit.if_modified_func () IS $body$
Track changes to a table at the statement and/or row level.

There is no parameter to disable logging of values. Add this trigger as
a 'FOR EACH STATEMENT' rather than 'FOR EACH ROW' trigger if you do not
want to log row values.

Note that the user name logged is the login role for the session. The audit trigger
cannot obtain the active role because it is reset by the SECURITY DEFINER invocation
of the audit trigger its self.
$body$;

CREATE OR REPLACE FUNCTION splitgraph_audit.audit_table (
    target_table regclass
)
    RETURNS void
    AS $body$
DECLARE
    stm_targets text = 'INSERT OR UPDATE OR DELETE OR TRUNCATE';
    _q_txt text;
BEGIN
    EXECUTE 'DROP TRIGGER IF EXISTS audit_trigger_row ON ' || target_table;
    _q_txt = 'CREATE TRIGGER audit_trigger_row AFTER INSERT OR UPDATE OR DELETE
     	ON ' || target_table || ' FOR EACH ROW EXECUTE PROCEDURE
     	splitgraph_audit.if_modified_func();';
    RAISE NOTICE '%', _q_txt;
    EXECUTE _q_txt;
END;
$body$
LANGUAGE 'plpgsql';

COMMENT ON FUNCTION splitgraph_audit.audit_table (regclass) IS $body$
Add auditing support to a table.

Arguments:
   target_table:     Table name, schema qualified if not on search_path
$body$;
