import unittest
from pathlib import Path
from bundestag import abgeordnetenwatch as aw
from bundestag import poll_clustering as pc
import pandas as pd


class TestPollClustering(unittest.TestCase):
    @classmethod
    def setUpClass(self):

        path = Path("./abgeordnetenwatch_data")
        col = "poll_title"
        nlp_col = f"{col}_nlp_processed"
        self.nlp_col = nlp_col
        df_polls = pd.read_parquet(
            path / "df_polls.parquet"
        )  # aw.get_polls_df(legislature_id, path=self.path)
        st = pc.SpacyTransformer()
        df_polls[nlp_col] = df_polls.pipe(st.clean_text, col=col)
        self.df_polls = df_polls
        st.fit(df_polls[nlp_col].values, mode="lda", num_topics=5)
        self.df_lda = st.transform_documents(df_polls[nlp_col])

    def test_text_cleaning(self):
        pc.test_cleaned_text(self.df_polls, col=self.nlp_col)

    def test_dense_topic_scores(self):
        pc.test_dense_topic_scores(self.df_lda.values)


if __name__ == "__main__":
    unittest.main()
