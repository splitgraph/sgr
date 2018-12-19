-- Add an unprivileged role that we can use to test RLS on the remote.
CREATE ROLE testuser LOGIN PASSWORD 'testpassword';