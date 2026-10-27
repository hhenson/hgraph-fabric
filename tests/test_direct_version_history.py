"""Top-first traversal with independently addressable version revision heads."""
import unittest
from dataclasses import fields
from unittest.mock import patch

from adversarial_cases import History, HistoryCache
from hgraph_fabric.consistent_cut import _revisions_backwards, find_consistent_cut
from hgraph_fabric.data_structures import DataRevisionDetails
from hgraph_fabric.data_subscription_graph import DataSubscriptionGraph
from hgraph_fabric.dependency_resolution import RevisionSearch


class DirectVersionHistoryTests(unittest.TestCase):
    def test_revision_record_contains_no_history_link(self):
        self.assertEqual([field.name for field in fields(DataRevisionDetails)],
                         ['dependencies', 'weak_dependencies', 'as_of'])

    def test_matching_top_does_not_load_zero_or_older_heads(self):
        history = History()
        history.add('x', 1)
        for version in range(1, 4):
            for count in range(8):
                history.add('a', version, count, dependencies=(('x', 1),))
        history.add('b', 1, dependencies=(('x', 1),))
        cache = HistoryCache(history)
        a, b = DataSubscriptionGraph('a'), DataSubscriptionGraph('b')
        a.proposed_revision, b.proposed_revision = history.heads['a'], history.heads['b']
        with patch.object(cache, 'get_latest_revision', wraps=cache.get_latest_revision) as heads:
            self.assertEqual(find_consistent_cut(a, b, cache), (('a', 3, 7), ('b', 1, 0)))
        self.assertFalse(any(revision[0] == 'a' and revision != ('a', 3, 7)
                             for kind, revision in cache.loads if kind == 'revision'))
        self.assertFalse(any(call.args[0][0] == 'a' for call in heads.call_args_list))

    def test_zero_probes_precede_older_heads_and_use_their_current_maximum(self):
        history = History()
        for version, target in ((1, 1), (2, 8), (3, 9)):
            history.add('a', version, dependencies=(('x', target),))
        # Version 1 keeps accepting inputs after versions 2 and 3 exist.
        for count in range(1, 6):
            history.add('a', 1, count, dependencies=(('x', count + 1),))
        cache = HistoryCache(history)
        events = []
        read, head = cache.get_revision, cache.get_latest_revision
        with patch.object(cache, 'get_revision', side_effect=lambda r: (events.append(('record', r)), read(r))[1]), \
             patch.object(cache, 'get_latest_revision', side_effect=lambda v: (events.append(('head', v)), head(v))[1]):
            candidates = list(_revisions_backwards(('a', 3, 0), cache, None, RevisionSearch(cache), {'x': 4}))
        self.assertEqual(candidates, [('a', 3, 0), ('a', 2, 0), ('a', 1, 5), ('a', 1, 3)])
        for version in (2, 1):
            self.assertLess(events.index(('record', ('a', version, 0))),
                            events.index(('head', ('a', version))))
        self.assertEqual([value for kind, value in events if kind == 'head'], [('a', 2), ('a', 1)])

    def test_proposed_ceiling_and_committed_floor_bound_the_count_search(self):
        history = History()
        for count in range(10):
            history.add('a', 2, count, dependencies=(('x', count + 1),))
        cache = HistoryCache(history)
        search = RevisionSearch(cache)
        with patch.object(cache, 'get_latest_revision', side_effect=AssertionError('No older head is admissible')):
            self.assertEqual(list(_revisions_backwards(('a', 2, 6), cache, ('a', 2, 3), search, {'x': 5})),
                             [('a', 2, 6), ('a', 2, 4)])
        loaded = [revision for kind, revision in cache.loads if kind == 'revision']
        self.assertTrue(all(3 <= revision[2] <= 6 for revision in loaded))

    def test_high_zero_does_not_hide_later_membership_change(self):
        history = History()
        history.add('a', 1, dependencies=(('x', 9),))
        history.add('a', 1, 1, dependencies=(('m', 1),))
        history.add('a', 1, 2, dependencies=(('x', 10),))
        cache = HistoryCache(history)
        # The intermediate path may admit a match, so an unverified interval
        # cannot be rejected using only its revision-zero value.
        self.assertEqual(list(RevisionSearch(cache).counts(('a', 1), 2, {'x': 3})), [2, 1, 0])
