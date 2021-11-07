from airflow.models.baseoperator import BaseOperator


class MapOperator(BaseOperator):
    template_fields = ['tweets']

    def __init__(self, tweets, *args, **kwargs):
        self.tweets = tweets
        super().__init__(*args, **kwargs)

    def execute(self, context):
        word_dict = dict()
        for tweet in self.tweets:
            for word in tweet.split():
                word_dict[word] = word_dict.get(word, 0) + 1
        return word_dict
