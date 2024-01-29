package BCDM::Criteria::COLL_DETAILS;
use strict;
use warnings;
use base 'BCDM::Criteria';

# this so that we know the criterionid for
# updates in the intersection table
sub _criterion { $BCDM::Criteria::COLL_DETAILS }

# this tests the criterion and returns
# boolean 0/1 depending on fail/pass. In
# addition, optional notes may be returned.
# Here, the criterion to assess is:
# 'Specimen has collection date, locality, and collector'
# Here we need to decide what the precise BCDM fields are
# to look at, and how to adjudicate different levels of
# richness. E.g. is everything OK if there's just a 
# country? Do we need multiple grades?
sub _assess {
    my $package = shift;
    my $record = shift;
    # TODO: implement!
    return 0, undef;
}

1;
