"""Publish and receive real records before resolving intermediate histories."""
from datetime import UTC, datetime, timedelta
from itertools import permutations
import unittest
import pyarrow as pa

from hgraph_fabric.data_fabric_session import DataFabricSession
from hgraph_fabric.dependency_resolution import RevisionSearch
from test_data_cache import MemoryDataCache


class Process:
    def __init__(self, world, name, inputs=()):
        self.world, self.name = world, name
        self.cache = MemoryDataCache()
        # Independent caches backed by the same persisted tables and records.
        self.cache.latest_values, self.cache.data_values, self.cache.revision_values = world.storage
        self.session = DataFabricSession(self.cache)
        self.subscriptions = {key: self.session.add_subscription(key) for key in inputs}
        if inputs:
            self.session.get_consistent_cut()
        self.received = []
        world.processes.append(self)

    def cut(self):
        return {key: sub.current_revision for key, sub in self.subscriptions.items()}

    def nodes(self):
        result = {}
        for key, sub in self.subscriptions.items():
            nodes = dict(sub.current(self.cache).nodes()) | {key: sub.current_revision[1]}
            for node, version in nodes.items():
                assert result.setdefault(node, version) == version
        return result

    def publish(self, key, version=None):
        self.nodes()  # The producer's actual selected input graphs must agree.
        deps = tuple(sub.current_revision[:2] for sub in self.subscriptions.values())
        for dep in deps:
            self.cache.get_version(dep)
        data = None if version is None else pa.table({'value': [1]}, metadata={b'version': str(version).encode()})
        self.world.time += timedelta(seconds=1)
        _, update = self.session.publish(data, id=key, dependencies=deps, as_of=self.world.time)
        revision = key, *update.revision
        assert revision in self.world.storage[2]  # Stored before notification.
        before = {p: p.cut() for p in self.world.processes}
        for process in self.world.processes:
            process.session.apply_update(update)
            process.received.append(revision)
        # No cut/read/publication interleaves a coordinated delivery round.
        for process in self.world.processes:
            assert all(process.cut()[key] >= old for key, old in before[process].items())
            process.nodes()
        return revision


class World:
    def __init__(self):
        self.storage = {}, {}, {}
        self.processes = []
        self.time = datetime(2026, 10, 24, tzinfo=UTC)

    def process(self, name, inputs=()):
        return Process(self, name, inputs)

    def history(self):
        source = self.process('source')
        source.publish('x', 1)
        middle = self.process('middle', ['x'])
        middle.publish('m', 100)
        producer = self.process('a', ['m'])
        producer.publish('a', 200)
        source.publish('x', 3)
        middle.publish('m')
        peer = self.process('b', ['x'])
        peer.publish('b', 100)
        # Publishing unchanged dependencies is a no-op; A keeps its value and
        # root revision even as its immediate M subscription receives revisions.
        producer.publish('a')
        for version in (5, 7):
            source.publish('x', version)
            middle.publish('m')
            producer.publish('a')
        return source, middle, producer, peer


class DependencyRevisionResolutionTests(unittest.TestCase):
    def test_fixed_root_resolves_older_intermediate_revision_after_delivery(self):
        for order in permutations(('a', 'b')):
            with self.subTest(order=order):
                world = World()
                world.history()
                reader = world.process('reader', order)
                self.assertEqual(reader.cut(), {'a': ('a', 200, 0), 'b': ('b', 100, 0)})
                self.assertEqual(reader.nodes()['x'], 3)
                a = reader.subscriptions['a']
                self.assertEqual(a.current_dependency_revisions[('m', 100)], ('m', 100, 1))
                # Explicit reads of the committed graph retain the choice.
                self.assertEqual(a.current(reader.cache).get_data_version_id('x'), ('x', 3))
                self.assertEqual(reader.cache.get_latest_revision(('m', 100)), ('m', 100, 3))

    def test_committed_choice_survives_notification_and_failed_proposal(self):
        world = World()
        source, middle, producer, peer = world.history()
        reader = world.process('reader', ['a', 'b'])
        before = reader.cut()
        source.publish('x', 9)
        middle.publish('m')
        self.assertEqual(reader.received[-1], ('m', 100, 4))
        self.assertEqual(reader.cut(), before)
        self.assertEqual(reader.nodes()['x'], 3)
        self.assertEqual(reader.subscriptions['a'].current_dependency_revisions[('m', 100)], ('m', 100, 1))
        # B publishes a new value from its real X@9 subscription. Resolution can
        # now select M's latest record and commit both graph choices together.
        peer.publish('b', 300)
        self.assertEqual(reader.cut()['b'], ('b', 300, 0))
        self.assertEqual(reader.nodes()['x'], 9)
        self.assertEqual(reader.subscriptions['a'].current_dependency_revisions[('m', 100)], ('m', 100, 4))

    def test_recursive_resolution_through_two_intermediates(self):
        world = World()
        world.history()
        outer = world.process('outer', ['m'])
        outer.publish('outer', 500)
        root = world.process('root', ['outer'])
        root.publish('root', 900)
        reader = world.process('reader', ['root', 'b'])
        self.assertEqual(reader.nodes()['x'], 3)
        choices = reader.subscriptions['root'].current_dependency_revisions
        self.assertEqual(choices[('outer', 500)], ('outer', 500, 0))
        self.assertEqual(choices[('m', 100)], ('m', 100, 1))

    def test_single_root_can_resolve_its_two_paths(self):
        world = World()
        source, middle, _, peer = world.history()
        # Obtain an X@3-dependent value and the retained M value through a real
        # input cut. The root records only these two immediate value versions.
        producer = world.process('diamond producer', ['m', 'b'])
        producer.publish('diamond', 600)
        reader = world.process('reader', ['diamond'])
        self.assertEqual(reader.nodes()['x'], 3)
        self.assertEqual(reader.subscriptions['diamond'].current_dependency_revisions[('m', 100)], ('m', 100, 1))

    def test_three_subscriptions_recheck_changed_dependency_choices(self):
        for order in permutations(('a', 'b', 'c')):
            world = World()
            source = world.process('source')
            source.publish('x', 1)
            middle = world.process('middle', ['x'])
            middle.publish('m', 100)
            producer = world.process('a', ['m'])
            producer.publish('a', 200)
            source.publish('x', 3)
            middle.publish('m')
            for key in ('b', 'c'):
                world.process(key, ['x']).publish(key, 100)
            source.publish('x', 7)
            middle.publish('m')
            reader = world.process('reader', order)
            self.assertEqual(reader.nodes()['x'], 3)
            self.assertEqual(reader.subscriptions['a'].current_dependency_revisions[('m', 100)], ('m', 100, 1))

    def test_both_intermediates_can_move_without_changing_either_root(self):
        world = World()
        source = world.process('source')
        source.publish('x', 1)
        m = world.process('m', ['x'])
        n = world.process('n', ['x'])
        m.publish('m', 100)
        n.publish('n', 200)
        world.process('a', ['m']).publish('a', 300)
        world.process('b', ['n']).publish('b', 400)
        source.publish('x', 2)
        n.publish('n')
        source.publish('x', 3)
        m.publish('m')
        reader = world.process('reader', ['a', 'b'])
        self.assertEqual(reader.nodes()['x'], 1)
        self.assertEqual(reader.cut(), {'a': ('a', 300, 0), 'b': ('b', 400, 0)})
        self.assertEqual(reader.subscriptions['a'].current_dependency_revisions[('m', 100)], ('m', 100, 0))
        self.assertEqual(reader.subscriptions['b'].current_dependency_revisions[('n', 200)], ('n', 200, 0))

    def test_rejected_root_update_keeps_committed_dependency_choices(self):
        world = World()
        source, middle, producer, peer = world.history()
        reader = world.process('reader', ['a', 'b'])
        before = reader.cut()
        a = reader.subscriptions['a']
        graph = a.current(reader.cache)
        choices = a.current_dependency_revisions
        source.publish('x', 9)  # M has not published a revision for X@9 yet.
        peer.publish('b', 300)
        self.assertEqual(reader.cut(), before)
        self.assertIs(a.current(reader.cache), graph)
        self.assertEqual(a.current_dependency_revisions, choices)
        self.assertEqual(reader.nodes()['x'], 3)

    def test_binary_count_bounds_use_immediate_records(self):
        world = World()
        source = world.process('source')
        source.publish('x', 1)
        middle = world.process('middle', ['x'])
        middle.publish('m', 100)
        for version in range(2, 34):
            source.publish('x', version)
            middle.publish('m')
        # The first probe is latest; subsequent counts are bounded by binary
        # searches over verified immutable immediate-dependency columns.
        search = RevisionSearch(middle.cache)
        self.assertEqual(list(search.counts(('m', 100), 32, {'x': 17})), [32, 16])

    def test_intermediate_exact_match_precedes_newer_omission(self):
        world = World()
        source = world.process('source')
        source.publish('x', 3)
        source.publish('z', 10)
        middle = world.process('middle', ['x'])
        middle.publish('m', 100)
        world.process('a', ['m']).publish('a', 200)
        world.process('b', ['x']).publish('b', 300)

        # Restart M with a different subscription, then restore its X path.
        # Each record is published from the restarted producer's actual cut.
        world.process('middle on z', ['z']).publish('m')
        source.publish('x', 7)
        world.process('middle on x', ['x']).publish('m')

        reader = world.process('reader', ['a', 'b'])
        self.assertEqual(reader.nodes()['x'], 3)
        self.assertNotIn('z', reader.nodes())
        self.assertEqual(reader.subscriptions['a'].current_dependency_revisions[('m', 100)], ('m', 100, 0))


if __name__ == '__main__':
    unittest.main()
