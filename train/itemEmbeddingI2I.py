import argparse
import numpy as np
import pandas as pd
import os
import faiss
from collections import defaultdict

class ItemEmbeddingI2I:
    def __init__(self, embed_path, normalize, threshold, top_n, output, index_type):
        self.embed_path = embed_path
        self.normalize = normalize
        self.threshold = threshold
        self.top_n = top_n
        self.output = output
        self.index_type = index_type

    def load_data(self):
        # Get the latest file in the embedding directory
        files = sorted(os.listdir(self.embed_path))
        latest_file = os.path.join(self.embed_path, files[-1])

        # Read the csv file, assuming it has columns 'item_id' and 'embedding'
        # The 'embedding' column contains embeddings as space-separated strings, e.g., "0.1 0.2 0.3 0.4"
        data = pd.read_csv(latest_file)
        ids = data['item_id'].values
        embeddings = np.vstack(data['embedding'].apply(lambda x: np.fromstring(x, sep=' ')))

        return ids, embeddings

    def calc_similarity(self, ids, embeddings):
        if self.normalize:
            faiss.normalize_L2(embeddings)

        if self.index_type == 'IndexFlatIP':
            index = faiss.IndexFlatIP(embeddings.shape[1])
        elif self.index_type == 'IndexFlatL2':
            index = faiss.IndexFlatL2(embeddings.shape[1])
        else:
            raise ValueError(f'Invalid index type: {self.index_type}')

        index.add(embeddings)

        scores, indices = index.search(embeddings, self.top_n + 1)

        item_pairs = defaultdict(list)
        for i, item_i in enumerate(ids):
            for j, score in zip(indices[i], scores[i]):
                if i != j and score >= self.threshold:
                    item_pairs[item_i].append((score, ids[j]))

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
        ids, embeddings = self.load_data()
        item_pairs = self.calc_similarity(ids, embeddings)
        self.output_result(item_pairs)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Calculate item to item similarity based on item embeddings.')
    parser.add_argument('--embed_path', type=str, required=True, help='Path to the item embedding files. The files should be in CSV format with columns "item_id" and "embedding". The "embedding" column contains embeddings as space-separated strings.')
    parser.add_argument('--normalize', action='store_true', default=False, help='Whether to normalize the item embeddings.')
    parser.add_argument('--threshold', type=float, default=0, help='Similarity threshold, pairs with similarity below this will not be kept.')
    parser.add_argument('--top_n', type=int, default=10, help='Number of most similar items to keep for each item.')
    parser.add_argument('--output', type=str, required=True, help='Path to output the result.')
    parser.add_argument('--index_type', type=str, default='IndexFlatIP', choices=['IndexFlatIP', 'IndexFlatL2'], help='Type of FAISS index.')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_arguments()
    i2i = ItemEmbeddingI2I(args.embed_path, args.normalize, args.threshold, args.top_n, args.output, args.index_type)
    i2i.run()