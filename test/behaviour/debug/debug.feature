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
    """

    Then verify answer size is: 2

  Scenario: test concludable

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa naming;
    """

    Then verify answer size is: 2

  Scenario: test other concludable

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa cartesian;
    """

    Then verify answer size is: 4

  Scenario: test concludable lookup

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa lookup;
    """

    Then verify answer size is: 1