import os
import pandas as pd
from annoy import AnnoyIndex
from sentence_transformers import SentenceTransformer
from yaspin import yaspin
from yaspin.spinners import Spinners

class ExchangeVectorDB:
    def __init__(self, model_name="sentence-transformers/all-MiniLM-L6-v2",
                 vector_dim=384, n_trees=50, rebuild=False):
        """
        Vector DB using Annoy with Hugging Face embeddings.
        Can combine multiple exchanges (NSE, BSE).
        """
        self.vector_dim = vector_dim
        self.n_trees = n_trees
        self.ticker_to_id = {}
        self.id_to_metadata = {}
        self.index = AnnoyIndex(vector_dim, 'angular')
        self.next_id = 0
        self.rebuild = rebuild

        print(f"🧠 Loading Hugging Face model: {model_name}")
        self.model = SentenceTransformer(model_name)

        if rebuild:
            print("♻️ Rebuilding Annoy vector DB (starting fresh)")

    def fetch_tickers(self, local_file, exchange_name):
        """
        Load ticker CSV (local) for a given exchange.
        Handles NSE and BSE formats.
        """
        abs_path = os.path.abspath(local_file)
        print(f"📂 Loading {exchange_name} tickers from: {abs_path}")
        df = pd.read_csv(local_file)
        df = df.rename(columns=lambda x: x.strip())

        if exchange_name == "BSE":
            # Map BSE columns to match NSE expected columns
            df['SYMBOL'] = df['Security Id']
            df['NAME OF COMPANY'] = df['Security Name']

        df['Exchange'] = exchange_name
        if not {"SYMBOL", "NAME OF COMPANY"}.issubset(df.columns):
            raise ValueError(f"{exchange_name} CSV must contain 'SYMBOL' and 'NAME OF COMPANY' columns")

        print(f"📊 {exchange_name} CSV contains {len(df)} tickers")
        return df[["SYMBOL", "NAME OF COMPANY", "Exchange"]]

    def _embed_text(self, text):
        """
        Convert text to embedding using Hugging Face model.
        """
        return self.model.encode(text.lower()).tolist()  # lowercase for consistency

    def build_vector_db(self, nse_file=None, bse_file=None):
        """
        Build combined vector DB from local CSVs for NSE and BSE.
        """
        combined_df = pd.DataFrame(columns=["SYMBOL", "NAME OF COMPANY", "Exchange"])

        if nse_file:
            df_nse = self.fetch_tickers(nse_file, "NSE")
            combined_df = pd.concat([combined_df, df_nse], ignore_index=True)

        if bse_file:
            df_bse = self.fetch_tickers(bse_file, "BSE")
            combined_df = pd.concat([combined_df, df_bse], ignore_index=True)

        print(f"🧩 Total tickers to index: {len(combined_df)}")

        with yaspin(Spinners.line, text="Building vector DB...") as spinner:
            for _, row in combined_df.iterrows():
                symbol = str(row["SYMBOL"]).lower()
                name = str(row["NAME OF COMPANY"]).lower()
                exchange = row["Exchange"]
                doc = f"{name} ({symbol}) - {exchange}"
                vec = self._embed_text(doc)

                self.ticker_to_id[f"{exchange}:{symbol}"] = self.next_id
                self.id_to_metadata[self.next_id] = {
                    "symbol": symbol.upper(),  # keep original case for display
                    "name": name.title(),
                    "exchange": exchange,
                    "document": doc
                }
                self.index.add_item(self.next_id, vec)
                self.next_id += 1

            self.index.build(self.n_trees)
            spinner.ok("✅ ")

        print(f"🧠 Vector DB built with {self.next_id} tickers")
        return combined_df

    def get_collection_count(self):
        return self.next_id

    def get_collection_contents(self, limit=5):
        entries = []
        for i in range(min(limit, self.next_id)):
            entries.append(self.id_to_metadata[i])
        return self.next_id, entries

    def search_ticker(self, query, n_results=10):
        vec = self._embed_text(query)
        nearest_ids = self.index.get_nns_by_vector(vec, n_results, include_distances=False)
        return [self.id_to_metadata[i] for i in nearest_ids]


if __name__ == "__main__":
    db = ExchangeVectorDB(rebuild=True, n_trees=50)

    print("\n🚀 Building vector DB from local CSV files...")
    combined_df = db.build_vector_db(
        nse_file="./data/ticker/nse.csv",
        bse_file="./data/ticker/bse.csv"
    )

    count = db.get_collection_count()
    print(f"🧠 Vector DB contains {count} entries")

    total, entries = db.get_collection_contents(limit=5)
    print(f"\n🔹 Showing top {len(entries)} entries:")
    for e in entries:
        print(f"   • {e['symbol']}: {e['name']} ({e['exchange']}) → {e['document']}")

    while True:
        query = input("\nEnter company name or symbol to search (or 'exit'): ").strip()
        if query.lower() == 'exit':
            break
        results = db.search_ticker(query)
        print(f"\n🔍 Search results for '{query}':")
        for r in results:
            print(f"   • {r['symbol']}: {r['name']} ({r['exchange']})")
