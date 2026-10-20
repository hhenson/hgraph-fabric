import unittest
from unittest.mock import Mock, call

from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_graph import DataEdgeGraph
from hgraph_fabric.data_structures import DataRevisionDetails
from hgraph_fabric.data_subscription_graph import DataSubscriptionGraph


class DataSubscriptionGraphTests(unittest.TestCase):
    def test_latest_and_current_select_independent_retained_graphs(self):
        subscription = DataSubscriptionGraph("prices")
        first_id = ("prices", 1, 1)
        second_id = ("prices", 2, 2)
        first = DataEdgeGraph()
        second = DataEdgeGraph([(("prices", 2), ("source", 1))])
        subscription._revision_graphs.update({first_id: first, second_id: second})
        subscription.current_revision = first_id
        subscription.latest_revision = second_id
        cache = Mock(spec=DataCache)
        self.assertIs(subscription.current(), first)
        self.assertIs(subscription.latest(), second)
        self.assertIs(subscription.revision(first_id, cache), first)
        self.assertIs(subscription.revision(second_id, cache), second)
        self.assertEqual(cache.mock_calls, [])

    def test_setting_current_prunes_older_versions_and_revisions(self):
        subscription = DataSubscriptionGraph("prices")
        cache = Mock(spec=DataCache)
        cache.get_revision.return_value = DataRevisionDetails(None, None, None)
        revision_ids = [
            ("prices", 1, 99),
            ("prices", 2, 1),
            ("prices", 2, 3),
            ("prices", 2, 5),
            ("prices", 3, 1),
        ]
        graphs = {
            revision_id: subscription.revision(revision_id, cache)
            for revision_id in revision_ids
        }
        subscription.latest_revision = revision_ids[-1]
        subscription.current_revision = ("prices", 2, 3)
        self.assertEqual(subscription.current_revision, ("prices", 2, 3))
        self.assertEqual(set(subscription._revision_graphs), set(revision_ids[2:]))
        self.assertIs(subscription.current(), graphs[("prices", 2, 3)])
        self.assertIs(subscription.latest(), graphs[revision_ids[-1]])
        subscription.current_revision = revision_ids[-1]
        self.assertEqual(subscription._revision_graphs, {
            revision_ids[-1]: graphs[revision_ids[-1]],
        })
        self.assertIs(subscription.current(), graphs[revision_ids[-1]])

    def test_clearing_current_retains_cached_graphs(self):
        subscription = DataSubscriptionGraph("prices")
        revision_id = ("prices", 1, 1)
        graph = DataEdgeGraph()
        subscription._revision_graphs[revision_id] = graph
        subscription.current_revision = revision_id
        subscription.current_revision = None
        self.assertIsNone(subscription.current_revision)
        self.assertEqual(subscription._revision_graphs, {revision_id: graph})
        with self.assertRaises(LookupError):
            subscription.current()

    def test_foreign_current_revision_does_not_prune_or_change_selection(self):
        subscription = DataSubscriptionGraph("prices")
        revision_id = ("prices", 1, 1)
        graph = DataEdgeGraph()
        subscription._revision_graphs[revision_id] = graph
        subscription.current_revision = revision_id
        with self.assertRaises(ValueError):
            subscription.current_revision = ("other", 9, 9)
        self.assertEqual(subscription.current_revision, revision_id)
        self.assertEqual(subscription._revision_graphs, {revision_id: graph})

    def test_unselected_and_unretained_revisions_raise_lookup_errors(self):
        subscription = DataSubscriptionGraph("prices")
        for accessor in (subscription.latest, subscription.current):
            with self.assertRaises(LookupError):
                accessor()
        subscription.latest_revision = ("prices", 1, 1)
        subscription.current_revision = ("prices", 1, 1)
        for accessor in (subscription.latest, subscription.current):
            with self.assertRaises(KeyError):
                accessor()

    def test_rejects_revision_for_another_data_id(self):
        subscription = DataSubscriptionGraph("prices")
        cache = Mock(spec=DataCache)
        with self.assertRaises(ValueError):
            subscription.revision(("other", 1, 1), cache)
        self.assertEqual(subscription._revision_graphs, {})
        self.assertIsNone(subscription.latest_revision)
        self.assertIsNone(subscription.current_revision)
        self.assertEqual(cache.mock_calls, [])

    def test_leaf_builds_empty_graph_and_ignores_weak_dependencies(self):
        subscription = DataSubscriptionGraph("prices")
        cache = Mock(spec=DataCache)
        cache.get_revision.return_value = DataRevisionDetails(None, (("weak", 1),), None)
        graph = subscription.revision(("prices", 1, 1), cache)
        self.assertEqual(set(graph), set())
        self.assertIs(subscription.revision(("prices", 1, 1), cache), graph)
        cache.get_revision.assert_called_once_with(("prices", 1, 1))
        cache.get_latest_revision.assert_not_called()
        cache.get_data.assert_not_called()
        self.assertIsNone(subscription.latest_revision)
        self.assertIsNone(subscription.current_revision)

    def test_expands_breadth_first_and_preserves_shared_dependency_edges(self):
        subscription = DataSubscriptionGraph("root")
        revisions = {
            ("root", 1, 7): DataRevisionDetails((("a", 1), ("b", 2)), None, None),
            ("a", 1, 3): DataRevisionDetails((("shared", 1),), (("weak", 1),), None),
            ("b", 2, 4): DataRevisionDetails((("shared", 1),), None, None),
            ("shared", 1, 9): DataRevisionDetails((("leaf", 3),), None, None),
            ("leaf", 3, 8): DataRevisionDetails((), None, None),
        }
        latest = {revision[:2]: revision for revision in revisions}
        cache = Mock(spec=DataCache)
        cache.get_revision.side_effect = revisions.__getitem__
        cache.get_latest_revision.side_effect = latest.__getitem__
        graph = subscription.revision(("root", 1, 7), cache)
        self.assertEqual(set(graph), {
            (("root", 1), ("a", 1)),
            (("root", 1), ("b", 2)),
            (("a", 1), ("shared", 1)),
            (("b", 2), ("shared", 1)),
            (("shared", 1), ("leaf", 3)),
        })
        self.assertEqual(
            cache.get_revision.call_args_list, [call(revision) for revision in revisions]
        )
        self.assertEqual(cache.get_latest_revision.call_args_list, [
            call(("a", 1)), call(("b", 2)), call(("shared", 1)), call(("leaf", 3)),
        ])
        cache.latest.assert_not_called()
        cache.get_data.assert_not_called()
        cache.reset_mock()
        self.assertIs(subscription.revision(("root", 1, 7), cache), graph)
        self.assertEqual(cache.mock_calls, [])

    def test_cycles_terminate_and_do_not_reselect_the_explicit_root_revision(self):
        subscription = DataSubscriptionGraph("root")
        revisions = {
            ("root", 1, 2): DataRevisionDetails((("a", 1), ("root", 1)), None, None),
            ("a", 1, 3): DataRevisionDetails((("root", 1),), None, None),
        }
        cache = Mock(spec=DataCache)
        cache.get_revision.side_effect = revisions.__getitem__
        cache.get_latest_revision.return_value = ("a", 1, 3)
        graph = subscription.revision(("root", 1, 2), cache)
        self.assertEqual(set(graph), {
            (("root", 1), ("a", 1)),
            (("root", 1), ("root", 1)),
            (("a", 1), ("root", 1)),
        })
        cache.get_latest_revision.assert_called_once_with(("a", 1))
        self.assertEqual(cache.get_revision.call_count, 2)

    def test_failed_expansion_is_not_retained_and_can_be_retried(self):
        subscription = DataSubscriptionGraph("root")
        revisions = {
            ("root", 1, 1): DataRevisionDetails((("a", 1),), None, None),
        }
        cache = Mock(spec=DataCache)
        cache.get_revision.side_effect = revisions.__getitem__
        cache.get_latest_revision.return_value = ("a", 1, 3)
        with self.assertRaises(KeyError):
            subscription.revision(("root", 1, 1), cache)
        self.assertEqual(subscription._revision_graphs, {})
        revisions[("a", 1, 3)] = DataRevisionDetails(None, None, None)
        graph = subscription.revision(("root", 1, 1), cache)
        self.assertEqual(set(graph), {(("root", 1), ("a", 1))})

    def test_conflicting_dependency_versions_do_not_retain_a_partial_graph(self):
        subscription = DataSubscriptionGraph("root")
        revisions = {
            ("root", 1, 1): DataRevisionDetails((("a", 1), ("b", 1)), None, None),
            ("a", 1, 1): DataRevisionDetails((("shared", 1),), None, None),
            ("b", 1, 1): DataRevisionDetails((("shared", 2),), None, None),
        }
        cache = Mock(spec=DataCache)
        cache.get_revision.side_effect = revisions.__getitem__
        cache.get_latest_revision.side_effect = lambda version: (*version, 1)
        with self.assertRaises(ValueError):
            subscription.revision(("root", 1, 1), cache)
        self.assertEqual(subscription._revision_graphs, {})
