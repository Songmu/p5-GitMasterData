requires 'DBI';
requires 'DBIx::FixtureLoader';
requires 'DBIx::Inspector';
requires 'DBIx::Sunny';
requires 'Git::Repository';
requires 'Mouse';
requires 'Path::Tiny';
requires 'SQL::Maker';
requires 'perl', '5.008001';

on configure => sub {
    requires 'Module::Build::Tiny', '0.035';
};

on test => sub {
    requires 'Test::Git';
    requires 'Test::More', '0.98';
    requires 'Test::Requires';
};
