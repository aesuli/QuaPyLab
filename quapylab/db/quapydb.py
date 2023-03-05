from abc import ABC, abstractmethod

from quapy import method
from quapy.classification.calibration import RecalibratedProbabilisticClassifierBase
from quapy.data import datasets


class QuaPyDB(ABC):

    @abstractmethod
    def validate(self, username: str, password: str) -> bool:
        pass

    def get_dataset_names(self):
        return {'Reviews sentiment': datasets.REVIEWS_SENTIMENT_DATASETS,
                'Twitter train': datasets.TWITTER_SENTIMENT_DATASETS_TRAIN,
                'Twitter test': datasets.TWITTER_SENTIMENT_DATASETS_TEST,
                'UCI': datasets.UCI_DATASETS,
                'LeQua 2022': datasets.LEQUA2022_TASKS,
                }

    def get_aggregative_algorithm_names(self):
        return [m.__name__ for m in method.AGGREGATIVE_METHODS]
        # aggregative_algorithms = {subclass for subclass in AggregativeQuantifier.__subclasses__()}
        # added = len(aggregative_algorithms)
        # while added:
        #     added = len(aggregative_algorithms)
        #     for algorithm in set(aggregative_algorithms):
        #         for subclass in algorithm.__subclasses__():
        #             aggregative_algorithms.add(subclass)
        #     added = len(aggregative_algorithms) - added
        # return [algorithm.__name__ for algorithm in aggregative_algorithms]

    def get_non_aggregative_algorithm_names(self):
        return [m.__name__ for m in method.NON_AGGREGATIVE_METHODS]

    def get_meta_algorithm_names(self):
        # m could be a string for QuaNet due to missing torch
        return [m.__name__ for m in method.META_METHODS if type(m) != str]

    def get_classification_algorithm_names(self):
        return ['MultinomialNB',
                'LinearSVC',
                'LogisticRegressionCV',
                'RandomForestClassifier',
                'SVMperf',
                'LowRankLogisticRegression',
                'LSTMnet',
                'CNNnet',
                ]

    def get_calibration_algorithm_names(self):
        return [subclass.__name__ for subclass in RecalibratedProbabilisticClassifierBase.__subclasses__()]

    # TODO allow use of RecalibratedProbabilisticClassifierBase + CalibratorFactory?
    # def get_calibrator_names(self):
    #     return [subclass.__name__ for subclass in CalibratorFactory.__subclasses__()]

    def get_sampling_protocol_names(self):
        return ["APP", "NPP", "UPP"]

    @abstractmethod
    def set_quantifier(self, quantifier, experiment_name, model_name, overwrite = False):
        pass

    @abstractmethod
    def get_quantifier(self, experiment_name, model_name):
        pass

    @abstractmethod
    def get_quantifier_names(self):
        pass

    @abstractmethod
    def add_job(self, function, kwargs):
        pass

    @abstractmethod
    def pop_pending_job(self):
        pass

    @abstractmethod
    def job_done(self, job_name):
        pass

    @abstractmethod
    def job_error(self, job_name):
        pass

    @abstractmethod
    def log_error(self, job_name, msg):
        pass