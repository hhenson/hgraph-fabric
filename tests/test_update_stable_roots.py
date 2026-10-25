"""At-latest roots stay fixed; intermediate choices can still resolve."""
import unittest
from unittest.mock import patch

from test_dependency_revision_resolution import World


class StableRootUpdateTests(unittest.TestCase):
    def test_duplicate_at_latest_root_updates_cache_without_preparing_upgrade(self):
        world = World()
        publisher = world.process('publisher')
        publisher.publish('a', 100)
        reader = world.process('reader', ['a'])
        manager = reader.session._subscriptions
        graph = reader.subscriptions['a'].current(reader.cache)
        with patch.object(manager, '_prepare_proposals', wraps=manager._prepare_proposals) as prepare:
            with patch.object(reader.cache, 'apply_update', wraps=reader.cache.apply_update) as cache_update:
                publisher.publish('a')
        cache_update.assert_called_once()
        prepare.assert_not_called()
        self.assertIs(reader.subscriptions['a'].current(reader.cache), graph)

    def test_at_latest_roots_reuse_stable_graphs_for_an_independent_advance(self):
        world = World()
        source = world.process('source')
        source.publish('x', 1)
        middle = world.process('middle', ['x'])
        middle.publish('m', 100)
        for key in ('a', 'c'):
            world.process(key, ['m']).publish(key, 200)
        source.publish('b', 400)
        reader = world.process('reader', ['a', 'c', 'b'])
        manager = reader.session._subscriptions
        graphs = {key: reader.subscriptions[key].current(reader.cache) for key in ('a', 'c')}
        self.assertEqual(reader.nodes()['x'], 1)
        sub = reader.subscriptions['a']
        with patch.object(sub, 'revision', wraps=sub.revision) as expand:
            with patch.object(manager, '_resolve', wraps=manager._resolve) as resolve:
                source.publish('b', 500)
        expand.assert_not_called()
        pairs = [set(args[1]) for args, _ in resolve.call_args_list]
        self.assertEqual(pairs, [{0, 2}, {1, 2}])
        for key, graph in graphs.items():
            self.assertIs(reader.subscriptions[key].current(reader.cache), graph)
            self.assertEqual(reader.subscriptions[key].current_revision, reader.subscriptions[key].latest_revision)
        self.assertEqual(reader.cut()['b'], ('b', 500, 0))
        self.assertEqual(reader.nodes()['x'], 1)

    def test_fixed_latest_roots_recheck_pairs_when_intermediate_choices_change(self):
        world = World()
        source, middle, _, peer = world.history()
        world.process('c', ['m']).publish('c', 500)
        reader = world.process('reader', ['a', 'c', 'b'])
        before = reader.cut()
        source.publish('x', 9)
        middle.publish('m')
        manager = reader.session._subscriptions
        with patch.object(manager, '_resolve', wraps=manager._resolve) as resolve:
            peer.publish('b', 300)
        pairs = [set(args[1]) for args, _ in resolve.call_args_list]
        self.assertIn({0, 1}, pairs)
        for key in ('a', 'c'):
            sub = reader.subscriptions[key]
            self.assertEqual(sub.current_revision, before[key])
            self.assertEqual(sub.current_revision, sub.latest_revision)
            self.assertEqual(sub.current_dependency_revisions[('m', 100)], ('m', 100, 4))
        self.assertEqual(reader.cut()['b'], ('b', 300, 0))
        self.assertEqual(reader.nodes()['x'], 9)

    def test_fixed_root_reconsiders_older_choices_without_a_new_dependency_notification(self):
        world = World()
        source, _, _, peer = world.history()
        world.process('d', ['b']).publish('d', 100)
        peer.publish('b', 300)
        reader = world.process('reader', ['a', 'b', 'd'])
        manager = reader.session._subscriptions
        a = reader.subscriptions['a']
        self.assertEqual(a.current_revision, a.latest_revision)
        self.assertEqual(a.current_dependency_revisions[('m', 100)], ('m', 100, 1))
        self.assertEqual(reader.cut()['b'], ('b', 100, 0))
        self.assertEqual(manager._dirty_subscriptions, set())
        # D restarts without its B dependency. M/r3 was already available at
        # initialisation: no new M notification will mark A's tree dirty.
        source.publish('d', 200)
        self.assertEqual(a.current_revision, ('a', 200, 0))
        self.assertEqual(a.current_dependency_revisions[('m', 100)], ('m', 100, 3))
        self.assertEqual(reader.cut()['b'], ('b', 300, 0))
        self.assertEqual(reader.cut()['d'], ('d', 200, 0))
        self.assertEqual(reader.nodes()['x'], 7)


if __name__ == '__main__':
    unittest.main()
