import unittest
from unittest.mock import Mock

from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_graph import DataEdgeGraph
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

    def test_uncached_construction_is_explicitly_deferred(self):
        subscription = DataSubscriptionGraph("prices")
        cache = Mock(spec=DataCache)
        with self.assertRaises(NotImplementedError):
            subscription.revision(("prices", 1, 1), cache)
        with self.assertRaises(ValueError):
            subscription.revision(("other", 1, 1), cache)
        self.assertEqual(subscription._revision_graphs, {})
        self.assertIsNone(subscription.latest_revision)
        self.assertIsNone(subscription.current_revision)
        self.assertEqual(cache.mock_calls, [])
