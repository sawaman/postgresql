
# Copyright (c) 2021-2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize publisher node
my $node = PostgreSQL::Test::Cluster->new('node');
$node->init(allows_streaming => 1);
$node->append_conf('postgresql.conf', 'synchronous_commit = on');
$node->start;

my ($ret, $stdout, $stderr);
$ret = $node->safe_psql('postgres', 'show wal_level');
print $ret;

$ret = $node->safe_psql('postgres', 'SELECT pg_get_logical_decoding_status()');
is($ret, 'disabled', 'logical decoding must be disabled');

# check if a logical replication slot cannot be craeted while logical
# decoding is disabled.
($ret, $stdout, $stderr) = $node->psql(
    'postgres',
    q[SELECT pg_create_logical_replication_slot('regression_slot1', 'test_decoding')]
    );
ok( $stderr =~ /ERROR:  logical decoding requires "wal_level" >= "logical"/,
    "logical replication slot cannot be created while logical decoding is disableed");

# Activate logical info logging.
$node->safe_psql('postgres',
		 'SELECT pg_activate_logical_decoding()');
$ret = $node->safe_psql('postgres', 'SELECT pg_get_logical_decoding_status()');
is($ret, 'ready', 'logical decoding gets activated');

# Now we can create a logical replication slot.
$ret = $node->safe_psql(
    'postgres',
    q[SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot1', 'test_decoding')]
    );
is($ret, 'init', 'successfully create logical replication slot');

# Cannot deactivate logical decoding while having valid slots.
($ret, $stdout, $stderr) = $node->psql(
    'postgres',
    q[SELECT pg_deactivate_logical_decoding()]
    );
ok( $stderr =~ /ERROR:  cannot deactivate logical decoding while having valid logical replication slots/,
    "cannot deactivate logical decoding while having valid logical slots");

$node->safe_psql(
    'postgres',
    q[
CREATE TABLE test (a int primary key, b int);
INSERT INTO test values (1, 100), (2, 200);
UPDATE test SET b = b + 10 WHERE a = 1;
    ]);

$ret = $node->safe_psql(
    'postgres',
    q[SELECT data FROM pg_logical_slot_get_changes('regression_slot1', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1');]
    );
is($ret, 'BEGIN
table public.test: INSERT: a[integer]:1 b[integer]:100
table public.test: INSERT: a[integer]:2 b[integer]:200
COMMIT
BEGIN
table public.test: UPDATE: a[integer]:1 b[integer]:110
COMMIT', 'logical decoding works fine');

# Drop the replication slot.
$node->safe_psql(
    'postgres',
    q[SELECT pg_drop_replication_slot('regression_slot1')]);

# Deactivate logical decoding.
$node->safe_psql(
    'postgres',
    q[SELECT pg_deactivate_logical_decoding()]);

$ret = $node->safe_psql('postgres', 'SELECT pg_get_logical_decoding_status()');
is($ret, 'disabled', 'logical decoding gets activated');

done_testing();

