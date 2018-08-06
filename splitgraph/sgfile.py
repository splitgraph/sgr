from hashlib import sha256

from splitgraph.commands import init, commit, pull, checkout
from splitgraph.constants import SplitGraphException
from splitgraph.meta_handler import get_current_head, get_canonical_snap_id, get_all_snap_parents


def parse_commands(lines):
    # obviously not a real parser
    commands = []

    merged_lines = []
    break_escaped = False
    for line in lines:
        if break_escaped:
            merged_lines[-1] += line
        else:
            merged_lines.append(line)
        if line.endswith('\\\n'):
            merged_lines[-1] = merged_lines[-1][:-2]
            break_escaped = True
        else:
            break_escaped = False

    for line in merged_lines:
        line = line.strip()
        if line.startswith('#') or not line:
            continue
        operator = line[:line.index(' ')]
        if operator in ('PULL', 'SOURCE', 'OUTPUT'):
            # PULL URL REMOTE_MOUNTPOINT LOCAL_MOUNTPOINT
            # SOURCE MOUNTPOINT [HASH]
            # OUTPUT MOUNTPOINT [HASH]
            operands = line.split()[1:]
        elif operator == 'SQL':
            operands = [line[line.index(' ') + 1:]]
        else:
            raise SplitGraphException("Unrecognized operator %s" % operator)
        commands.append((operator, operands))
    return commands


def _canonicalize(sql):
    return ' '.join(sql.lower().split())


def _combine_hashes(hashes):
    return sha256(''.join(hashes)).hexdigest()


def execute_commands(conn, commands):
    sources = {}
    output = None

    for i, com in enumerate(commands):
        operator, operands = com
        print "-> %d/%d %s" % (i + 1, len(commands), operator + ' ' + ' '.join(operands))
        if operator == 'PULL':
            conn_string = operands[0]
            remote_mountpoint = operands[1]
            local_mountpoint = operands[2]
            pull(conn, conn_string, remote_mountpoint, local_mountpoint)
            snap_id = get_all_snap_parents(conn, local_mountpoint)[-1][0]
            checkout(conn, local_mountpoint, snap_id)
        if operator == 'SOURCE':
            mountpoint = operands[0]

            # Check mountpoint actually exists
            if get_current_head(conn, mountpoint) is None:
                raise SplitGraphException("%s does not exist. Has it been mounted?" % mountpoint)

            if len(operands) > 1:
                if operands[1] == 'LATEST':
                    # Pending there being timestamps in the commits, we're just fetching the last record in the db.
                    # This might be undefined behaviour, yeah.
                    snap_id = get_all_snap_parents(conn, mountpoint)[-1][0]
                else:
                    snap_id = get_canonical_snap_id(conn, mountpoint, operands[1])
            else:
                snap_id = get_current_head(conn, mountpoint)
            print "Using source mountpoint %s snapshot %s" % (mountpoint, snap_id)
            sources[mountpoint] = snap_id
        elif operator == 'OUTPUT':
            output = operands[0]
            print "Committing results to mountpoint %s" % output
            try:
                get_current_head(conn, output)
            except SplitGraphException:
                # Output doesn't exist, create it.
                init(conn, output)

            # By default, use the current output HEAD. Check if it's overridden.
            if len(operands) > 1:
                output_head = get_canonical_snap_id(conn, output, operands[1])
                checkout(conn, output, output_head)

        elif operator == 'SQL':
            if output is None:
                raise SplitGraphException("Error: no OUTPUT specified. OUTPUT mountpoint is snapshot during file execution.")
            # Calculate the hash of the layer we are trying to create.
            # It's the xor of all input + output layer hashes as well as the hash of the SQL command itself.
            sql_command = _canonicalize(operands[0])
            output_head = get_current_head(conn, output)
            target_hash = _combine_hashes(sources.values() + [output_head, sha256(sql_command).hexdigest()])

            print ', '.join('%s:%s' % (mp, si[:12]) for mp, si in sources.iteritems()) + ', %s:%s' % (output, output_head[:12]) + \
                ' -> %s:%s' % (output, target_hash[:12])

            # Have we already calculated this hash?
            try:
                checkout(conn, output, target_hash)
                print "Using the cache."
            except SplitGraphException:
                # Check out all the input layers
                for mountpoint, snap_id in sources.iteritems():
                    print "Using %s snap %s" % (mountpoint, snap_id[:12])
                    checkout(conn, mountpoint, snap_id)
                print "Executing SQL..."
                with conn.cursor() as cur:
                    cur.execute(sql_command)

                commit(conn, output, target_hash)
                conn.commit()
