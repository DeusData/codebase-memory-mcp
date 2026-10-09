"""Contract tests for complete LLVM frontend profile text records."""
import dataclasses
import hashlib
from pathlib import Path
import sys
import threading
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts/test-impact'))
import profile_text_parser as p

CAPTURES = ROOT / 'tests/fixtures/profile_text'
BIG = p.ParseLimits(2_000_000, 20_000, 100_000, 1_000_000)
MAX = 18446744073709551615


def wire(name=b'f', function_hash=24, counters=(0,)):
    return (name + b'\n# Func Hash:\n' + str(function_hash).encode() +
            b'\n# Num Counters:\n' + str(len(counters)).encode() +
            b'\n# Counter Values:\n' +
            b''.join(str(x).encode() + b'\n' for x in counters) + b'\n')


def values(rows):
    return tuple((x.raw_name, x.function_hash, x.counters) for x in rows)


class ProfileTextContract(unittest.TestCase):
    def test_native_dense_and_sparse_captures(self):
        expected = {
            'cold-dense.proftext': ('b8f1bd2ca4c7dd9e2ca567b410897ca8f5c0c648a1d73648f58724a708a93daf',
                ((b'cbm_profile_probe_hit',24,(0,)), (b'cbm_profile_probe_never',24,(0,)),
                 (b'main',490457921399630188,(1,0,1,1,1,0,0)))),
            'hot-dense.proftext': ('3f77bbbda8d76b4d440109d24f0431f8b9bca6628bc0241470d1dbbb2c2695ec',
                ((b'cbm_profile_probe_hit',24,(1,)), (b'cbm_profile_probe_never',24,(0,)),
                 (b'main',490457921399630188,(1,0,1,1,0,1,1)))),
            'cold-sparse.proftext': ('50979a6198fcfcdabcc3f09057009be6cc54053eea4af581aadf4ec91b3199a7',
                ((b'main',490457921399630188,(1,0,1,1,1,0,0)),)),
            'hot-sparse.proftext': ('72bf344ccd0897cfe3c5625657e6912d645deb1fb495d6dea551b40b89f9dc68',
                ((b'cbm_profile_probe_hit',24,(1,)),
                 (b'main',490457921399630188,(1,0,1,1,0,1,1))))}
        for name, (digest, oracle) in expected.items():
            with self.subTest(name=name):
                data = (CAPTURES/name).read_bytes()
                self.assertEqual(hashlib.sha256(data).hexdigest(), digest)
                self.assertEqual(values(p.parse_profile_text(data, BIG)), oracle)

    def test_full_identity_order_and_all_counters(self):
        rows = ((b'\xff:name\t #',MAX,(0,MAX)), (b'z',10,(0,)),
                (b'z',2,(0,0)), (b' a ',0,(1,)), (b'# Func Hash:',24,(0,)))
        data = b''.join(wire(*r) for r in rows)
        parsed = p.parse_profile_text(data, BIG)
        self.assertEqual(values(parsed), (rows[3], rows[4], rows[2], rows[1], rows[0]))
        self.assertEqual(values(p.parse_profile_text(b''.join(wire(*r) for r in reversed(rows)), BIG)),
                         values(parsed))
        self.assertTrue(any(parsed[-1].counters))
        self.assertEqual(parsed[-1].counters[0], 0)

    def test_exact_grammar_and_unsigned_decimals(self):
        good = wire()
        malformed = [b'', b'\n', b'\n'+good, good+b'\n', good+b'# unknown\n',
            good.replace(b'\n',b'\r\n'), good.replace(b'f\n',b'f\x00\n',1),
            good.replace(b'# Func Hash:',b'# Function Hash:'),
            good.replace(b'# Num Counters:',b'# Num Counters: '),
            good.replace(b'# Counter Values:',b'# Values:'),
            good.replace(b'\n1\n# Counter Values:',b'\n0\n# Counter Values:'),
            good.replace(b'\n0\n\n',b'\n0\n0\n\n'),
            good.replace(b'\n0\n\n',b'\n\n'), b':irprof\n'+good]
        for decimal in (b'', b'00', b'024', b'+24', b'-1', b' 24', b'24 ', b'0x18',
                        b'2.4', b'2e1', b'18446744073709551616'):
            malformed.append(good.replace(b'\n24\n',b'\n'+decimal+b'\n'))
            malformed.append(good.replace(b'\n0\n\n',b'\n'+decimal+b'\n\n'))
        self.assertEqual(values(p.parse_profile_text(good, BIG)), ((b'f',24,(0,)),))
        for index,data in enumerate(malformed):
            with self.subTest(index=index):
                with self.assertRaises(p.MalformedProfileError):
                    p.parse_profile_text(data, BIG)

    def test_all_proper_prefixes_and_suffix_rejection(self):
        data = wire(b'prefix:name',MAX,(0,1,MAX))
        self.assertEqual(len(p.parse_profile_text(data,BIG)),1)
        for length in range(len(data)):
            with self.subTest(length=length):
                with self.assertRaises(p.MalformedProfileError):
                    p.parse_profile_text(data[:length],BIG)
        for tail in (b'x',b'\x00',wire()[:-1],b'# Num Value Kinds:\n0\n'):
            with self.assertRaises(p.MalformedProfileError):
                p.parse_profile_text(data+tail,BIG)

    def test_duplicate_identity_and_hash_distinction(self):
        one=wire(b'name',1,(0,)); other=wire(b'other',1,(1,))
        self.assertEqual(len(p.parse_profile_text(one+wire(b'name',2,(0,0)),BIG)),2)
        for duplicate in (one,wire(b'name',1,(1,)),wire(b'name',1,(0,0))):
            with self.assertRaises(p.MalformedProfileError):
                p.parse_profile_text(one+other+duplicate,BIG)

    def test_inclusive_resource_limits_and_claimed_counts(self):
        data=wire(b'abc',1,(0,1))+wire(b'de',2,(0,))+wire(b'f',3,(0,0))
        exact=(len(data),3,5,6)
        self.assertEqual(len(p.parse_profile_text(data,p.ParseLimits(*exact))),3)
        for index in range(4):
            limited=list(exact);limited[index]-=1
            with self.subTest(index=index):
                with self.assertRaises(p.LimitExceededError):
                    p.parse_profile_text(data,p.ParseLimits(*limited))
        claimed=b'f\n# Func Hash:\n0\n# Num Counters:\n18446744073709551615\n# Counter Values:\n'
        with self.assertRaises(p.LimitExceededError):
            p.parse_profile_text(claimed,BIG)
        with self.assertRaises(p.MalformedProfileError):
            p.parse_profile_text(claimed.replace(str(MAX).encode(),str(MAX+1).encode()),BIG)

    def test_argument_types_and_limit_validation(self):
        good=wire()
        for invalid in (None,good.decode(),bytearray(good),memoryview(good)):
            with self.assertRaises(p.InvalidArgumentError):
                p.parse_profile_text(invalid,BIG)
        for value in (0,-1,True,1.0,None,sys.maxsize+1):
            for index in range(4):
                limits=[1000,1000,1000,1000];limits[index]=value
                with self.assertRaises(p.InvalidArgumentError):
                    p.parse_profile_text(good,p.ParseLimits(*limits))
        with self.assertRaises(p.InvalidArgumentError):
            p.parse_profile_text(good,None)
        for callback in (False,1,'callback'):
            with self.assertRaises(p.InvalidArgumentError):
                p.parse_profile_text(good,BIG,cancel=callback)
        for answer in (None,0,1,'true'):
            with self.assertRaises(p.InvalidArgumentError):
                p.parse_profile_text(good,BIG,cancel=lambda _:answer)

    def test_cancellation_and_exception_propagation_are_owner_local(self):
        marker=object();calls=[]
        def cancelled(context):
            self.assertIs(context,marker);calls.append(context);return True
        with self.assertRaises(p.ParseCancelledError):
            p.parse_profile_text(b'not a profile',BIG,cancel=cancelled,cancel_context=marker)
        self.assertEqual(len(calls),1)
        class CallbackFailure(Exception): pass
        sentinel=CallbackFailure('callback marker')
        def explode(_): raise sentinel
        with self.assertRaises(CallbackFailure) as caught:
            p.parse_profile_text(wire(),BIG,cancel=explode)
        self.assertIs(caught.exception,sentinel)
        big=wire(b'x'*100_000,MAX,(0,1))
        for cutoff in (2,3):
            state=[0]
            def later(_): state[0]+=1;return state[0]>=cutoff
            with self.assertRaises(p.ParseCancelledError):
                p.parse_profile_text(big,BIG,cancel=later)
            self.assertEqual(state[0],cutoff)
        self.assertEqual(values(p.parse_profile_text(wire(),BIG)),((b'f',24,(0,)),))

    def test_cancellation_sweep_including_sort_and_final_publication(self):
        rows=tuple((b'q'*5000+bytes((97+i,)),i,(0,1)) for i in reversed(range(8)))
        data=b''.join(wire(*row) for row in rows)
        calls=[0]
        def observe(_): calls[0]+=1;return False
        expected=values(p.parse_profile_text(data,BIG,cancel=observe))
        self.assertEqual(expected,tuple(reversed(rows)))
        self.assertGreater(calls[0],3)
        self.assertLessEqual(calls[0],512)  # Bounds the deterministic sweep itself.
        for cutoff in range(1,calls[0]+1):
            state=[0]
            def cancel_at(_): state[0]+=1;return state[0]>=cutoff
            with self.subTest(cutoff=cutoff):
                with self.assertRaises(p.ParseCancelledError):
                    p.parse_profile_text(data,BIG,cancel=cancel_at)
                self.assertEqual(state[0],cutoff)
        self.assertEqual(values(p.parse_profile_text(data,BIG)),expected)

    def test_held_owner_survives_concurrent_cancellation(self):
        self.assertEqual(values(p.parse_profile_text(wire(),BIG)),((b'f',24,(0,)),))
        entered=threading.Event();release=threading.Event();out=[];errors=[]
        def run():
            try:
                owner=p.parse_profile_text(wire(b'held',MAX,(0,1)),BIG)
                entered.set()
                if not release.wait(10): raise AssertionError('release gate failed')
                out.append(owner)
            except BaseException as error: errors.append(error)
        thread=threading.Thread(target=run);thread.start()
        try:
            self.assertTrue(entered.wait(10),'entry gate failed')
            other=p.parse_profile_text(wire(b'other',2,(1,)),BIG)
            with self.assertRaises(p.ParseCancelledError):
                p.parse_profile_text(wire(b'cancelled'),BIG,cancel=lambda _:True)
            self.assertEqual(values(other),((b'other',2,(1,)),))
        finally:
            release.set();thread.join(10)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
        self.assertEqual(len(out),1);self.assertEqual(values(out[0]),((b'held',MAX,(0,1)),))

    def test_result_immutability_and_independent_owners(self):
        a=p.parse_profile_text(wire(b'a',1,(0,MAX)),BIG)
        b=p.parse_profile_text(wire(b'b',2,(1,)),BIG)
        self.assertIsInstance(a,tuple);self.assertIsInstance(a[0].counters,tuple)
        with self.assertRaises((dataclasses.FrozenInstanceError,AttributeError)):
            a[0].raw_name=b'changed'
        self.assertEqual(values(a),((b'a',1,(0,MAX)),));self.assertEqual(values(b),((b'b',2,(1,)),))
        start=threading.Event();out=[None,None];errors=[]
        def run(slot):
            try:
                if not start.wait(10): raise AssertionError('start gate failed')
                out[slot]=p.parse_profile_text(wire(bytes((97+slot,)),slot,(0,)),BIG)
            except BaseException as error: errors.append(error)
        threads=[threading.Thread(target=run,args=(i,)) for i in range(2)]
        for thread in threads: thread.start()
        start.set()
        for thread in threads: thread.join(10);self.assertFalse(thread.is_alive())
        self.assertEqual(errors,[])
        self.assertEqual(values(out[0]),((b'a',0,(0,)),));self.assertEqual(values(out[1]),((b'b',1,(0,)),))


if __name__ == '__main__':
    unittest.main(verbosity=2)
