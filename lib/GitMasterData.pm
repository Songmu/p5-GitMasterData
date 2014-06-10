package GitMasterData;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use Mouse;
use utf8;

use DBI;
use DBIx::FixtureLoader;
use DBIx::Inspector;
use File::Spec;
use Git::Repository;
use SQL::Maker;
use Path::Tiny qw/path/;

has version_table => (
    is       => 'ro',
    isa      => 'Str',
    default  => 'master_data_version',
);

has master_file_ext => (
    is       => 'ro',
    isa      => 'Str',
    default  => 'csv',
);

has master_dir => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has work_tree => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has dbh => (
    is       => 'ro',
    isa      => 'DBI::db',
    lazy     => 1,
    default  => sub {
        my $self = shift;
        require DBIx::Sunny;
        DBIx::Sunny->connect(@{ $self->dsn });
    },
);

has dsn => (
    is  => 'ro',
    isa => 'ArrayRef',
);

has verbose => (
    is      => 'ro',
    isa     => 'Bool',
    default => 1,
);

has _fixture_loader => (
    is => 'ro', lazy => 1,
    default => sub {
        DBIx::FixtureLoader->new(
            dbh         => shift->dbh,
            delete      => 1,
            bulk_insert => 1,
        );
    },
    handles => [qw/transaction_manager/],
);

has _git => (
    is      => 'ro',
    lazy    => 1,
    default => sub {
        Git::Repository->new( work_tree => shift->work_tree )
    }
);

has _sql_maker => (
    is      => 'ro',
    lazy    => 1,
    default => sub {
        SQL::Maker->new( driver => shift->dbh->{Driver}{Name} )
    },
);

has _inspector => (
    is      => 'ro',
    lazy    => 1,
    default => sub {
        DBIx::Inspector->new( dbh => shift->dbh )
    },
);

no Mouse;

sub check_version {
    my ($self) = @_;
    $self->database_version eq $self->master_dir_version;
}

sub deploy {
    my ($self) = @_;

    my $txn = $self->transaction_manager->txn_scope;

    $self->dbh->do(<<"SQL");
CREATE TABLE @{[ $self->version_table ]} (
    version VARCHAR(40) NOT NULL
);
SQL

    $self->_insert_master_datas;

    my ($sql, @bind) = $self->_sql_maker->insert($self->version_table, {
        version => $self->master_dir_version,
    });
    $self->dbh->do($sql, {}, @bind);

    $txn->commit;
}

sub upgrade {
    my ($self) = @_;

    my $txn = $self->transaction_manager->txn_scope;

    $self->_insert_master_datas;
    $self->_update_database_version;
    $txn->commit;
}

sub datas_to_be_updated {
    my $self = shift;

    my $database_version = $self->database_version;

    my %file_map;
    if ($database_version) {
        %file_map = $self->_diff_file_map($self->database_version, $self->master_dir_version);
    }
    else {
        my $ext = quotemeta $self->master_file_ext;
        %file_map = map {
            my $file = File::Spec->abs2rel($_->absolute, $self->work_tree . '');
            $file =~ /\.$ext$/ ? ("$file" => 'A') : ()
        } path($self->master_dir)->children;
    }

    %file_map;
}

sub _insert_master_datas {
    my $self = shift;

    my $ext   = quotemeta $self->master_file_ext;
    my %file_map = $self->datas_to_be_updated;
    for my $data_file (keys %file_map) {
        my $table = path($data_file)->basename;
           $table =~ s/\.$ext$//;
        if ($self->_inspector->table( $table )) {
            if ($file_map{$data_file} ne 'D') {
                printf "load %s ... ", $table if $self->verbose;
                $self->_fixture_loader->load_fixture(File::Spec->catfile($self->work_tree . '', $data_file));
                print 'done.' ."\n" if $self->verbose;
            }
            else {
                printf "delete %s ... ", $table if $self->verbose;
                $self->dbh->do("DELETE FROM $table");
                print 'done.' . "\n" if $self->verbose;
            }
        }
        else {
            print "table $table is not exists and skipped.\n" if $self->verbose;
        }
    }
}

sub master_dir_version {
    my ($self) = @_;
    $self->_git->run('log', '-n', '1', '--pretty=format:%H', '--', $self->master_dir);
}

sub database_version {
    my $self = shift;

    my $version = eval {
        open my $fh, '>', \my $stderr;
        local *STDERR = $fh;
        my ($ver) = $self->dbh->selectrow_array("SELECT version FROM @{[ $self->version_table ]}");
        $ver;
    };
    !$@ && $version;
}

sub _diff_file_map {
    my ($self, $old_version, $new_version) = @_;
    my $ext = quotemeta $self->master_file_ext;
    my @lines = $self->_git->run('diff', '--name-status', $old_version, $new_version, $self->master_dir);
    map {
        my ($status, $path) = split /\s+/, $_;
        $path =~ /\.$ext$/ ? ($path => $status) : ();
    } @lines;
}

sub _update_database_version {
    my ($self) = @_;

    my ($sql, @bind) = $self->_sql_maker->update($self->version_table, {
        version => $self->master_dir_version,
    });
    $self->dbh->do($sql, {}, @bind);
}

1;
__END__

=encoding utf-8

=head1 NAME

GitMasterData - It's new $module

=head1 SYNOPSIS

    use GitMasterData;

=head1 DESCRIPTION

GitMasterData is ...

=head1 LICENSE

Copyright (C) Songmu.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Songmu E<lt>y.songmu@gmail.comE<gt>

=cut

