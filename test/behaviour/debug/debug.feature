# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
Feature: Debugging Space

  Background:

    Given typedb starts
    Given connection opens with default authentication
  # Paste any scenarios below for debugging.
  # Do not commit any changes to this file.


  Scenario: test some stuff

    Given reasoning schema
    """
    define
    name sub attribute, value string;
    person sub entity, owns name;

    """
   Given reasoning data
    """
    insert
    $p1 isa person, has name "Alice";
    $p2 isa person, has name "Bob";
    """

    Given reasoning query
    """
    match
    $p isa person, has name $n;
    """

    Then verify answer size is: 2