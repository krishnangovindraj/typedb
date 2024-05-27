# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
Feature: Debugging Space

  Background:

    Given typedb starts
    Given connection opens with default authentication
  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.

    Given reasoning schema
    """
    define
    name sub attribute, value string;
    person sub entity, owns name;

    naming sub relation, relates person, relates name;
    person plays naming:person;
    name plays naming:name;

    cartesian sub relation, relates person, relates name;
    person plays cartesian:person;
    name plays cartesian:name;

    lookup sub relation, relates person, relates name;
    person plays lookup:person;
    name plays lookup:name;

    lookup-complement sub relation, relates person, relates name;
    person plays lookup-complement:person;
    name plays lookup-complement:name;


    rule naming-rule:
    when {
      $p isa person, has name $n;
    } then {
      (person: $p, name: $n) isa naming;
    };

    rule cartesian-rule:
    when {
      $p isa person; $n isa name;
    } then {
      (person: $p, name: $n) isa cartesian;
    };

    rule lookup-rule:
    when {
      $p isa person, has name $n;
      $n "Doesnt exist";
    } then {
      (person: $p, name: $n) isa lookup;
    };

    rule lookup-complement:
    when {
      $p isa person; $n isa name;
      not {(person: $p, name: $n) isa lookup;};
    } then {
      (person: $p, name: $n) isa lookup-complement;
    };

    """
    Given reasoning data
    """
    insert
    $p1 isa person, has name $n1; $n1 "Alice";
    $p2 isa person, has name $n2; $n2 "Bob";
    (person: $p1, name: $n2) isa lookup;
    """


  Scenario: test retrievable
    Given reasoning query
    """
    match
    $p isa person, has name $n;
    get;
    """

    Then verify answer size is: 2

  Scenario: test concludable

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa naming;
    get;
    """

    Then verify answer size is: 2

  Scenario: test other concludable

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa cartesian;
    get;
    """

    Then verify answer size is: 4

  Scenario: test concludable lookup

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa lookup;
    get;
    """

    Then verify answer size is: 1


  Scenario: test negated

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa lookup-complement;
    get;
    """

    Then verify answer size is: 3



  Scenario: test cycles
    Given reasoning schema
     """
     define
     node sub attribute, value long;
     edge sub relation, relates from, relates to;
     node plays edge:from, plays edge:to;
     direct-edge sub edge;

     rule transitive-edge:
     when {
        (from: $a, to: $b) isa edge;
        (from: $b, to: $c) isa direct-edge;
     } then {
        (from: $a, to: $c) isa edge;
     };
     """

    Given reasoning data
    """
    insert
      $n1 1 isa node;
      $n2 2 isa node;
      $n3 3 isa node;

      (from: $n1, to: $n2) isa direct-edge;
      (from: $n2, to: $n3) isa direct-edge;
    """

    Given reasoning query

    """
    match
      (from: $x, to: $y) isa edge;
    get;
    """

    Then verify answer size is: 3


#  Scenario: SHRUNK - the types of entities in inferred relations can be used to make further inferences
#    Given reasoning schema
#      """
#      define
#      place sub entity,
#        owns name,
#        plays location-hierarchy:subordinate,
#        plays location-hierarchy:superior,
#        plays big-location-hierarchy:big-subordinate,
#        plays big-location-hierarchy:big-superior;
#
#      location-hierarchy sub relation,
#        relates subordinate,
#        relates superior;
#
##      big-place sub place,
##        plays big-location-hierarchy:big-subordinate,
##        plays big-location-hierarchy:big-superior;
#
#      big-location-hierarchy sub location-hierarchy,
#        relates big-subordinate as subordinate,
#        relates big-superior as superior;
#
#      rule transitive-location: when {
#        (subordinate: $x, superior: $y) isa location-hierarchy;
#        (subordinate: $y, superior: $z) isa location-hierarchy;
#      } then {
#        (subordinate: $x, superior: $z) isa location-hierarchy;
#      };
#
#      rule if-a-big-thing-is-in-a-big-place-then-its-a-big-location: when {
#        (subordinate: $x, superior: $y) isa location-hierarchy;
#      } then {
#        (big-subordinate: $x, big-superior: $y) isa big-location-hierarchy;
#      };
#      """
#    Given reasoning data
#      """
#      insert
#      $x isa place, has name "Mount Kilimanjaro";
#      $y isa place, has name "Tanzania";
#      $z isa place, has name "Africa";
#
#      (subordinate: $x, superior: $y) isa location-hierarchy;
#      (subordinate: $y, superior: $z) isa location-hierarchy;
#      """
#    Given verifier is initialised
#    Given reasoning query
#      """
#      match (subordinate: $x, superior: $y) isa big-location-hierarchy;
#      """
#    Then verify answer size is: 1234
##    Then verify answers are sound
##    Then verify answers are complete


  Scenario: ORIGINAL - the types of entities in inferred relations can be used to make further inferences
    Given reasoning schema
      """
      define
      place sub entity,
        owns name,
        plays location-hierarchy:subordinate,
        plays location-hierarchy:superior;

      location-hierarchy sub relation,
        relates subordinate,
        relates superior;

      big-place sub place,
        plays big-location-hierarchy:big-subordinate,
        plays big-location-hierarchy:big-superior;

      big-location-hierarchy sub location-hierarchy,
        relates big-subordinate as subordinate,
        relates big-superior as superior;

      rule transitive-location: when {
        (subordinate: $x, superior: $y) isa location-hierarchy;
        (subordinate: $y, superior: $z) isa location-hierarchy;
      } then {
        (subordinate: $x, superior: $z) isa location-hierarchy;
      };

      rule if-a-big-thing-is-in-a-big-place-then-its-a-big-location: when {
        $x isa big-place;
        $y isa big-place;
        (subordinate: $x, superior: $y) isa location-hierarchy;
      } then {
        (big-subordinate: $x, big-superior: $y) isa big-location-hierarchy;
      };
      """
    Given reasoning data
      """
      insert
      $x isa big-place, has name "Mount Kilimanjaro";
      $y isa place, has name "Tanzania";
      $z isa big-place, has name "Africa";

      (subordinate: $x, superior: $y) isa location-hierarchy;
      (subordinate: $y, superior: $z) isa location-hierarchy;
      """
    Given verifier is initialised
    Given reasoning query
      """
      match (subordinate: $x, superior: $y) isa big-location-hierarchy; get;
      """
    Then verify answer size is: 1
#    Then verify answers are sound
#    Then verify answers are complete
#
