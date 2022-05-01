from collections import OrderedDict
from itertools import permutations
from math import pow, ceil


class Balancer():
    def __init__(self, shards: int, cluster: dict):
        """
        Base init
        :param shards:
        :param cluster:
        """
        self.shards = shards
        self.cluster_raw = cluster
        self.total_cluster_nodes = sum(cluster.values())
        # Sort and order our data
        _sorted_cluster = {k: v for k, v in sorted(cluster.items(), key=lambda item: item[1])}
        self.o_sorted_cluster = OrderedDict(_sorted_cluster)
        self.permutation_bucket = list(permutations(range(shards)))
        self.best_case = self.percentile(self.total_cluster_nodes / self.shards, self.total_cluster_nodes)
        self.max_score_ceiling = pow(2, ceil(self.shards/2))
        self.score_repeat_count = 0
        self.score_board = {}
        self.score_count = 0

    def percentile(self, x: int, y: int):
        """
        Return x as a percentile of y
        :param x:
        :param y:
        :return:
        """
        return round((x/y)*100, 2)

    def seed_partition_map(self):
        """
        Create a partition map with number of shards and sequentially distribute the clusters to
        each partition index to be balanced further. as we are sorting our clusters by node count, this function
        will get us close, but not exact. hence we do further balancing later
        :return:
        """
        # seed a partition map
        self.partition_map = OrderedDict()
        for i in range(self.shards):
            self.partition_map[i] = {}

            # partition map sample:
            # {0: {'prod-cluster-19': 3, 'prod-cluster-18': 7, 'prod-cluster-4': 20, 'prod-cluster-14': 40,
            #      'prod-cluster-6': 100},
            #  1: {'prod-cluster-7': 5, 'prod-cluster-8': 8, 'prod-cluster-15': 20, 'prod-cluster-2': 55, 'prod-cluster-11': 100},
            #  2: {'prod-cluster-20': 6, 'prod-cluster-17': 8, 'prod-cluster-3': 25, 'prod-cluster-13': 70,
            #      'prod-cluster-1': 120},
            #  3: {'prod-cluster-9': 7, 'prod-cluster-5': 10, 'prod-cluster-16': 30, 'prod-cluster-12': 90,
            #      'prod-cluster-10': 120}}

        split = 0
        for c in self.o_sorted_cluster.keys():
            #  populate the map
            if split >= self.shards:
                split = 0
            self.partition_map[split][c] = self.o_sorted_cluster[c]
            split += 1

    def write_score(self):
        """
        update our score board
        :return:
        """
        self.score_board[self.score_count] = {}
        for p in self.partition_map.keys():
            self.score_board[self.score_count][p] = self.percentile(sum(self.partition_map[p].values()), self.total_cluster_nodes)

        if self.score_count > 0:
            if self.score_board[self.score_count] == self.score_board[self.score_count - 1]:
                self.score_repeat_count += 1
                print(f"{self.score_repeat_count} {self.max_score_ceiling}")
            else:
                self.score_repeat_count = 0

        self.score_count += 1


    def make_balance_tuple(self, idx_lst: list):
        """
        Now we create a bunch of tuples which will match (largest, smallest), (larger, smaller) and so on. we use this
        to play balancing game later
        :return:
        """
        _bal_idx = idx_lst
        self.bal_tup_full = []
        while _bal_idx:
            bal_tup = []
            try:
                bal_tup.append(_bal_idx.pop(0))
            except IndexError:
                pass
            try:
                bal_tup.append(_bal_idx.pop(-1))
            except IndexError:
                pass
            self.bal_tup_full.append(bal_tup)
        #     sample balanc tupple
        #     [[0, 3], [1, 2]]

    def run(self):
        """
        Start balancing the tupple pairs
        :return:
        """
        self.seed_partition_map()

        # Run balancing for all permutations of the given partition map
        for perm in self.permutation_bucket:
            if self.score_repeat_count < self.max_score_ceiling:

                self.make_balance_tuple(list(perm))

                for bal_tup in self.bal_tup_full:
                    self.make_balance_dict(bal_tup)
                    self.get_adjusted_delta()
                    self.shuffle_clusters()
                    self.finalize_pair()
                self.print_results()
                self.write_score()
            else:
                print(f"We repeated the optimal score {self.max_score_ceiling} times. stopping.")
                return

    def make_balance_dict(self, bal_tup: list):
        """
        Create a balancing dict for the given cluster pair, we'll use this to sort and balance between the cluster pair
        :return:
        """
        print(f"balancing pair {bal_tup}")
        # create an empty dict and add tupple values as keys, and from the partition_map get the total node count
        # by the tupple key and set that as value
        bal_dict = {}
        for idx in bal_tup:
            bal_dict[idx] = sum(self.partition_map[idx].values())
        # {0: 200, 3:250} - example data

        # sort the above dict and order it
        sorted_bal_dict = {k: v for k, v in sorted(bal_dict.items(), key=lambda item: item[1])}
        # this is our dict to work with for balancing this pair of clusters
        self.o_sorted_bal_dict = OrderedDict(sorted_bal_dict)
        print(f"o_sorted_bal_dict:{self.o_sorted_bal_dict}")
        # print some data pre-sorting
        for k in self.o_sorted_bal_dict.keys():
            # print(
            #     f"pre-optimized sorted_bal[{k}]: {self.o_sorted_bal_dict[k]}, {round((sum(self.partition_map[k].values()) / self.total_cluster_nodes), 2) * 100}%")
            print(
                f"pre-optimized sorted_bal[{k}]: {self.o_sorted_bal_dict[k]}, {self.percentile(sum(self.partition_map[k].values()), self.total_cluster_nodes)}%")

    def get_adjusted_delta(self):
        """
        Get the adjusted delta for the given cluster pair so we can start balancing. we use the self.o_sorted_bal_dict
        to get the delta
        :return:
        """
        #  since are a sorted dict, we can take first index as the small chunk and last index as teh large chunk
        #  get the node count difference between large and small chunks

        delta = self.o_sorted_bal_dict[list(self.o_sorted_bal_dict.keys())[-1]] - self.o_sorted_bal_dict[
            list(self.o_sorted_bal_dict.keys())[0]]
        print(f"delta: {delta}")
        # adjusted delta
        # this is to handle cases where one cluster is has tooooo many nodes compared to others and the diff is 40%-50%
        #  for exmaple 1118 - 559 = 559, in this case we need to make sure the balancing equally divides the delta,
        #  if the delta is >40% <=50% we'll split the delta by 2 to be even.
        try:
            lowwater_total = self.o_sorted_bal_dict[list(self.o_sorted_bal_dict.keys())[0]]
            highwater_total = self.o_sorted_bal_dict[list(self.o_sorted_bal_dict.keys())[1]]
            adj_delta_pc = self.percentile(lowwater_total, highwater_total)

            # get adjusted delta if the diff is 40~50%
            if adj_delta_pc > 40 and adj_delta_pc <= 50:
                self.adjusted_delta = delta / 2
            else:
                self.adjusted_delta = delta
        except IndexError:
            # handle when odd number of clusters are passed
            self.adjusted_delta = delta

    def shuffle_clusters(self):
        """
        start shuffling the clusters and create a shuffle cluster list
        :return:
        """
        self.shuffle_keys = []
        shuffle_count = 0
        for cluster in self.partition_map[list(self.o_sorted_bal_dict.keys())[-1]]:
            # check each cluster in the large chunk and see if we can move a cluster to the smaller chunk
            # if node count is larger than the diff we cant move it
            if self.partition_map[list(self.o_sorted_bal_dict.keys())[-1]][cluster] > self.adjusted_delta:
                # since we use a dict sorted by node count as value, we can be sure if we reach this we have reached
                # an index after which teh clusters are larger than our diff delta
                pass
                # print("We are done here..")
            else:
                # this is a cluster that can be shuffled. start counting the nodes as we shuffle
                shuffle_count += self.partition_map[list(self.o_sorted_bal_dict.keys())[-1]][cluster]
                #  if we have enough nodes to meet the adjusted_delta
                if shuffle_count > self.adjusted_delta:
                    shuffle_count_sum = 0
                    # check if the last cluster node count is more balancing than the previous shuffle clusters combined
                    if self.partition_map[list(self.o_sorted_bal_dict.keys())[-1]][cluster] < self.adjusted_delta:
                        for s in self.shuffle_keys:
                            shuffle_count_sum += self.partition_map[list(self.o_sorted_bal_dict.keys())[-1]][s]
                        if shuffle_count_sum < self.partition_map[list(self.o_sorted_bal_dict.keys())[-1]][cluster]:
                            # this single cluster is a better shuffle than the combined total of previous shuffles
                            #  we collected. update the shuffle list to clear() and add only this cluster. we should
                            #  be done at this point
                            print("optimimize shuffle...")
                            self.shuffle_keys.clear()
                            self.shuffle_keys.append(cluster)

                else:
                    #  add the cluster to shuffle list and move on, we have more shuffling to do
                    self.shuffle_keys.append(cluster)

    def finalize_pair(self):
        """
        Move around the cluster to finalize
        :return:
        """
        print(f"clusters to shuffle: {self.shuffle_keys}")
        print('balancing...')
        for s in self.shuffle_keys:
            self.partition_map[list(self.o_sorted_bal_dict.keys())[0]][s] = self.partition_map[
                list(self.o_sorted_bal_dict.keys())[-1]].pop(s)

    def print_results(self):
        for tier in self.partition_map.keys():
            print(
                f"optimized tier: {tier} - {sum(self.partition_map[tier].values())}, "
                f"{self.percentile(sum(self.partition_map[tier].values()), self.total_cluster_nodes)}%")
