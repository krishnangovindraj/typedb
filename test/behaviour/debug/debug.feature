#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

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


  Scenario: test negated

    Given reasoning query
    """
    match
      (person: $p, name: $n) isa lookup-complement;
    """

    Then verify answer size is: 3