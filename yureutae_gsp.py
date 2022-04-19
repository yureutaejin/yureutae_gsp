"""
===============================================
GSP (Generalized Sequential Pattern) algorithm
===============================================

GSP algorithm made with Python3 to deal with arrays as transactions.

Example:

raw_transactions = [
        [['a', 'b'], ['c'], ['f', 'g'], ['g'], ['e']],
        [['a', 'd'], ['c'], ['b'], ['a', 'b', 'e', 'f']],
        [['a'], ['b'], ['f', 'g'], ['e']],
        [['b'], ['f', 'g']]
]
"""


import logging
import multiprocessing as mp
from itertools import chain
from itertools import product
from itertools import combinations

import numpy as np

class yureutae_GSP:

    def __init__(self, raw_transactions):
        self.freq_patterns = []
        self._pre_processing(raw_transactions)

    def _pre_processing(self, raw_transactions):
        '''
        Prepare the data

        Parameters:
                raw_transactions: the data that it will be analysed
        '''
        self.max_size = max([len(item) for item in raw_transactions])   # 트랜잭션들 길이 중 가장 긴 것
        self.transactions = [tuple(list(i)) for i in raw_transactions]  # 트랜잭션 튜플화

        counts = {} # length_1 종류 찾아내서 개수로 정리 (트랜잭션 내부 중복 허용)
        for transaction in raw_transactions:
            for element in transaction:
                if (len(element) > 1):
                    for i in element:
                        if (i not in counts):
                            counts[i] = 1
                        else:
                            counts[i] += 1
                else:
                    if (element[0] not in counts):
                        counts[element[0]] = 1
                    else:
                        counts[element[0]] += 1

        # length_1 key만 정리
        self.unique_candidates = [tuple([k]) for k, c in counts.items()]

    def _is_slice_in_list(self, s, l):
        len_s = len(s)
        for i in range(len(l)-len_s + 1):
            temp_list = list(chain.from_iterable(l[i:len_s + i]))

            if (len_s == 1):
                for j in s:
                    if j in temp_list:
                        return True

        if len_s >= 2:
            index_list = []
            before_index = -1
            for j in s:
                for element in l:
                    if (j in element) and (before_index<l.index(element)):
                        index_list.append(l.index(element))
                        before_index = l.index(element)
                        break

            index_list = list(set(index_list))  # 동시 발생 우선 제외

            if len(index_list) == len_s:
                return True
            elif len(index_list) != len_s:
                return False

        elif (type(s)==set):    # 동시 발생 추가
            for element in l:
                if s == set(element):
                    return True


    def _calc_frequency(self, results, item, minsup):   # 수정
        # The number of times the item appears in the transactions
        frequency = len(
            [transaction for transaction in self.transactions if self._is_slice_in_list(item, transaction)]
        )
        if frequency >= minsup:
            results[item] = frequency
        return results

    def _support(self, items, minsup=0):
        '''
        The support count (or simply support) for a sequence is defined as
        the fraction of total data-sequences that "contain" this sequence.
        (Although the word "contains" is not strictly accurate once we
        incorporate taxonomies, it captures the spirt of when a data-sequence
        contributes to the support of a sequential pattern.)

        Parameters
                items: set of items that will be evaluated
                minsup: minimum support
        '''
        results = mp.Manager().dict()
        pool = mp.Pool(processes=mp.cpu_count())

        for item in items:
            pool.apply_async(self._calc_frequency,
                             args=(results, item, minsup))
        pool.close()
        pool.join()

        return dict(results)

    def _print_status(self, run, candidates):
        logging.debug("""
        Run {}
        There are {} candidates.
        The candidates have been filtered down to {}.\n"""
                      .format(run,
                              len(candidates),
                              len(self.freq_patterns[run - 1])))

    def search(self, minsup=0.2):
        '''
        Run GSP mining algorithm

        Parameters
                minsup: minimum support
        '''
        assert (0.0 < minsup) and (minsup <= 1.0)
        minsup = len(self.transactions) * minsup # minsup 백분위로 입력받고 곱해서 갯수로 변경

        # the set of frequent 1-sequence: all singleton sequences
        # (k-itemsets/k-sequence = 1) - Initially, every item in DB is a candidate
        candidates = self.unique_candidates

        # scan transactions to collect support count for each candidate
        # sequence & filter
        self.freq_patterns.append(self._support(candidates, minsup))

    # ---------------cleear------------------------------------------

        # (k-itemsets/k-sequence = 1)
        k_items = 1

        self._print_status(k_items, candidates)

        # repeat until no frequent sequence or no candidate can be found
        while (len(self.freq_patterns[k_items - 1]) and (k_items + 1 <= self.max_size)):
            k_items += 1

            # Generate candidate sets Ck (set of candidate k-sequences) -
            # generate new candidates from the las "best" candidates filtered
            # by minimum support
            items = np.unique(
                list(set(self.freq_patterns[k_items - 2].keys()))
            )

            candidates = list(product(items, repeat=k_items)) # 동시에 발생하지 않은 candidate
            candidates_com = set(combinations(items, k_items))
            for tuple_item in candidates_com:
                candidates.append(set(tuple_item))


            # candidate pruning - eliminates candidates who are not potentially
            # frequent (using support as threshold)
            self.freq_patterns.append(self._support(candidates, minsup))
            self._print_status(k_items, candidates)
        return self.freq_patterns[:-1]

if __name__=='__main__':
    from gsppy.gsp import GSP

    raw_transactions = [
        [['a', 'b'], ['c'], ['f', 'g'], ['g'], ['e']],
        [['a', 'd'], ['c'], ['b'], ['a', 'b', 'e', 'f']],
        [['a'], ['b'], ['f', 'g'], ['e']],
        [['b'], ['f', 'g']]
    ]

    result = yureutae_GSP(raw_transactions)

    print("yureutae_GSP")
    print(result.search(0.5))