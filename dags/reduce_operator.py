from airflow.models.baseoperator import BaseOperator


class ReduceOperator(BaseOperator):
    template_fields = ["word_dicts"]

    def __init__(self, word_dicts, *args, **kwargs):
        self.word_dicts = word_dicts
        super().__init__(*args, **kwargs)

    def execute(self, context):
        res_dict = dict()
        for word_dict in self.word_dicts:
            for word in word_dict:
                res_dict[word] = res_dict.get(word, 0) + word_dict[word] 
        return res_dict
