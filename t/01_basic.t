use strict;
use warnings;
use utf8;
use Test::More;
use Test::Git;

use DBI;
use Path::Tiny;
use File::Copy qw/copy/;
use Git::Repository;
use GitMasterData;
use Test::Requires 'DBD::SQLite';

my $r = test_repository();

my $datadir = path($r->work_tree)->child('data');
$datadir->mkpath;

my $dbh = DBI->connect("dbi:SQLite::memory:", '', '', {
    RaiseError     => 1,
    sqlite_unicode => 1,
});

$dbh->do(q{
CREATE  TABLE item (
  id   INTEGER PRIMARY KEY,
  name VARCHAR(255)
)
});

$dbh->do(q{
CREATE  TABLE item2 (
  id   INTEGER PRIMARY KEY,
  name VARCHAR(255)
)
});

my $gmd = GitMasterData->new(
    master_dir    => $datadir->stringify,
    work_tree     => $r->work_tree,
    dbh           => $dbh,
);

isa_ok $gmd, 'GitMasterData';
ok !$gmd->database_version;

copy 't/data/item.csv', $datadir->child('item.csv') .'';
$r->run('add', 'data/item.csv');
$r->run('commit', '-m', 'add item.csv');

my $prev_version;

subtest 'deploy' => sub {
    $gmd->deploy;
    $prev_version = $gmd->database_version;
    ok $prev_version;

    my ($count) = $gmd->dbh->selectrow_array(
        "SELECT COUNT(id) FROM item"
    );
    is $count, 2;
};

copy 't/data/item-ascii.csv', $datadir->child('item2.csv') .'';
$r->run('add', 'data/item2.csv');
$r->run('commit', '-m', 'add item2.csv');

subtest 'skill count 0' => sub {
    my ($count) = $gmd->dbh->selectrow_array(
        "SELECT COUNT(id) FROM item2"
    );
    is $count, 0;
};

$gmd->upgrade;
subtest 'inserted' => sub {
    my ($count) = $gmd->dbh->selectrow_array(
        "SELECT COUNT(id) FROM item2"
    );
    is $count, 2;
};

copy 't/data/item-update.csv', $datadir->child('item.csv') .'';
$r->run('add', 'data/item.csv');
$r->run('commit', '-m', 'update item.csv');

$r->run('rm', $datadir->child('item2.csv').'');
$r->run('commit', '-m', 'remove item2.csv');

$gmd->upgrade;

subtest 'updated' => sub {
    my ($count) = $gmd->dbh->selectrow_array(
        "SELECT COUNT(id) FROM item2"
    );
    is $count, 0;

    ($count) = $gmd->dbh->selectrow_array(
        "SELECT COUNT(id) FROM item"
    );
    is $count, 3;
};

copy 't/data/item-ascii.csv', $datadir->child('item2.csv') .'';
$r->run('add', 'data/item2.csv');
$r->run('commit', '-m', 'add item2.csv');
copy 't/data/item-invalid.csv', $datadir->child('item.csv') .'';
$r->run('add', 'data/item.csv');
$r->run('commit', '-m', 'add item.csv');

eval {
    $gmd->upgrade;
};
print "\n"; # for tap output
ok $@;

subtest 'not changed' => sub {
    my ($count) = $gmd->dbh->selectrow_array(
        "SELECT COUNT(id) FROM item2"
    );
    is $count, 0;

    ($count) = $gmd->dbh->selectrow_array(
        "SELECT COUNT(id) FROM item"
    );
    is $count, 3;
};

done_testing;
