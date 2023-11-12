from src import alpha_train


def execute_alpha_train(**kwargs):
    print(f'execute_alpha_train kwargs: {kwargs}')
    alpha_train.train_model(**kwargs)
