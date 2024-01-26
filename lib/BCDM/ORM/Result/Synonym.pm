use utf8;
package BCDM::ORM::Result::Synonym;

# Created by DBIx::Class::Schema::Loader
# DO NOT MODIFY THE FIRST PART OF THIS FILE

=head1 NAME

BCDM::ORM::Result::Synonym

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';

=head1 TABLE: C<synonyms>

=cut

__PACKAGE__->table("synonyms");

=head1 ACCESSORS

=head2 synonymid

  data_type: 'integer'
  is_auto_increment: 1
  is_nullable: 0

=head2 name

  data_type: 'text'
  is_nullable: 0

=head2 targetid

  data_type: 'integer'
  is_nullable: 0

=cut

__PACKAGE__->add_columns(
  "synonymid",
  { data_type => "integer", is_auto_increment => 1, is_nullable => 0 },
  "name",
  { data_type => "text", is_nullable => 0 },
  "targetid",
  { data_type => "integer", is_nullable => 0 },
);

=head1 PRIMARY KEY

=over 4

=item * L</synonymid>

=back

=cut

__PACKAGE__->set_primary_key("synonymid");


# Created by DBIx::Class::Schema::Loader v0.07049 @ 2024-01-26 13:01:24
# DO NOT MODIFY THIS OR ANYTHING ABOVE! md5sum:TZTWX+HKSkfMfIBKha4lEg


# You can replace this text with custom code or comments, and it will be preserved on regeneration
1;
