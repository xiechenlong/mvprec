import argparse
import heapq
import json
import pandas as pd
import numpy as np
from collections import defaultdict
from itertools import combinations
from multiprocessing import Pool, cpu_count

class ItemCFI2I:
    def __init__(self, path, sim_type, top_n, sim_params, output):
        self.path = path
        self.sim_type = sim_type
        self.top_n = top_n
        self.sim_params = sim_params
        self.output = output

    def load_data(self):
        data = pd.read_csv(self.path)
        return data

    def calc_similarity(self, group):
        user_items = defaultdict(set)
        item_users = defaultdict(set)

        for user_id, item_id in group[['user_id', 'item_id']].values:
            user_items[user_id].add(item_id)
            item_users[item_id].add(user_id)

        item_weights = defaultdict(float)
        if self.sim_type == 'wb_cosine':
            user_weights = {user_id: 1 / np.log2(3 + len(items)) for user_id, items in user_items.items()}
        
            for item, users in item_users.items():
                item_weights[item] = np.sqrt(sum(user_weights[user_id] for user_id in users))

        alpha = self.sim_params.get('alpha', 1)
        items = list(item_users.keys())
        item_pairs = defaultdict(list)

        for i, item_i in enumerate(items):
            for j in range(i+1, len(items)):
                item_j = items[j]
                common_users = item_users[item_i] & item_users[item_j]
                if not common_users:
                    continue

                if self.sim_type == 'jaccard':
                    sim = len(common_users) / len(item_users[item_i] | item_users[item_j])
                elif self.sim_type == 'cosine':
                    sim = len(common_users) / np.sqrt(len(item_users[item_i]) * len(item_users[item_j]))
                elif self.sim_type == 'wb_cosine':
                    sim = sum(user_weights[user_id] for user_id in common_users) / (item_weights[item_i] * item_weights[item_j])
                elif self.sim_type == 'swing':
                    sim = 0
                    for user_u, user_v in combinations(common_users, 2):
                        sim += 1 / (alpha + len(user_items[user_u] & user_items[user_v]))
                else:
                    raise ValueError('Invalid similarity type.')

                # Keep top_n similarities for each item
                if len(item_pairs[item_i]) < self.top_n:
                    heapq.heappush(item_pairs[item_i], (sim, item_j))
                else:
                    heapq.heappushpop(item_pairs[item_i], (sim, item_j))

                if len(item_pairs[item_j]) < self.top_n:
                    heapq.heappush(item_pairs[item_j], (sim, item_i))
                else:
                    heapq.heappushpop(item_pairs[item_j], (sim, item_i))

        return item_pairs

    def output_result(self, item_pairs):
        item1 = []
        item2 = []
        scores = []
        for item, pairs in item_pairs.items():
            for score, pair_item in pairs:
                item1.append(item)
                item2.append(pair_item)
                scores.append(score)
        result = pd.DataFrame({'item1': item1, 'item2': item2, 'score': scores})
        result.to_csv(self.output, index=False, mode='a')

    def run(self):
        data = self.load_data()
        groups = data.groupby('group_id')
        with Pool(processes=cpu_count()) as pool:
            results = pool.map(self.calc_similarity, [group for _, group in groups])
            for item_pairs in results:
                self.output_result(item_pairs)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Calculate item to item similarity.')
    parser.add_argument('--path', type=str, required=True, help='Path to the i2i sample file.')
    parser.add_argument('--sim_type', type=str, required=True, choices=['jaccard', 'cosine', 'wb_cosine', 'swing'], help='Type of similarity to be calculated.')
    parser.add_argument('--top_n', type=int, default=10, help='Number of most similar items to keep for each item.')
    parser.add_argument('--sim_params', type=json.loads, default={}, help='Additional parameters needed for similarity calculation.')
    parser.add_argument('--output', type=str, required=True, help='Path to output the result.')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_arguments()
    i2i = ItemCFI2I(args.path, args.sim_type, args.top_n, args.sim_params, args.output)
    i2i.run()