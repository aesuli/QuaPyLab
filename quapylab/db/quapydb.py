from abc import ABC, abstractmethod
from enum import Enum

from pandas import DataFrame


class JobStatus(Enum):
    creating = 'creating'
    pending = 'pending'
    running = 'running'
    done = 'done'
    error = 'error'


LABEL_COLUMN_NAMES = ['label', 'class']
TEXT_COLUMN_NAMES = ['text', 'document', 'content']


def get_label_column_name(df: DataFrame):
    label_column_name = None
    for name in LABEL_COLUMN_NAMES:
        if name in df.columns:
            label_column_name = name
            break

    if label_column_name is None:
        raise AttributeError(
            f'Dataset {name} does not contain a label column using one of these names: {", ".join(LABEL_COLUMN_NAMES)}')

    return label_column_name


def get_text_column_name(df: DataFrame):
    text_column_name = None
    for name in TEXT_COLUMN_NAMES:
        if name in df.columns:
            text_column_name = name
            break

    return text_column_name


def get_data_column_names(df: DataFrame):
    data_column_names = list()
    for name in df.columns:
        if name not in LABEL_COLUMN_NAMES and name not in TEXT_COLUMN_NAMES:
            data_column_names.append(name)

    return data_column_names


class QuaPyDB(ABC):

    @abstractmethod
    def validate(self, username: str, password: str) -> bool:
        pass

    @abstractmethod
    def set_dataset_from_file(self, name, file, overwrite):
        pass

    @abstractmethod
    def get_dataset(self, name) -> DataFrame:
        pass

    @abstractmethod
    def get_dataset_info(self, name):
        pass

    @abstractmethod
    def delete_dataset(self, name):
        pass

    @abstractmethod
    def get_dataset_names(self):
        pass

    @abstractmethod
    def get_dataset_count(self):
        pass

    @abstractmethod
    def set_quantifier(self, name, quantifier, overwrite=False):
        pass

    @abstractmethod
    def delete_quantifier(self, name):
        pass

    @abstractmethod
    def get_quantifier(self, name):
        pass

    @abstractmethod
    def get_quantifier_names(self):
        pass

    @abstractmethod
    def get_quantifier_count(self):
        pass

    @abstractmethod
    def create_job(self, function, kwargs):
        pass

    @abstractmethod
    def pop_pending_job(self):
        pass

    @abstractmethod
    def set_job_done(self, job_id):
        pass

    @abstractmethod
    def set_job_failed(self, job_id):
        pass

    @abstractmethod
    def log_job_error(self, job_id, msg, append=True):
        pass

    @abstractmethod
    def get_job_ids(self):
        pass

    @abstractmethod
    def get_job_info(self, job_id):
        pass

    @abstractmethod
    def get_job_count(self):
        pass

    @abstractmethod
    def delete_job(self, job_id):
        pass

    @abstractmethod
    def rerun_job(self, job_id):
        pass

    @abstractmethod
    def get_job_error_log(self, job_id):
        pass

    # def get_aggregative_algorithm_names(self):
    #     return [m.__name__ for m in method.AGGREGATIVE_METHODS]
    #     # aggregative_algorithms = {subclass for subclass in AggregativeQuantifier.__subclasses__()}
    #     # added = len(aggregative_algorithms)
    #     # while added:
    #     #     added = len(aggregative_algorithms)
    #     #     for algorithm in set(aggregative_algorithms):
    #     #         for subclass in algorithm.__subclasses__():
    #     #             aggregative_algorithms.add(subclass)
    #     #     added = len(aggregative_algorithms) - added
    #     # return [algorithm.__name__ for algorithm in aggregative_algorithms]
    #
    # def get_non_aggregative_algorithm_names(self):
    #     return [m.__name__ for m in method.NON_AGGREGATIVE_METHODS]
    #
    # def get_meta_algorithm_names(self):
    #     # m could be a string for QuaNet due to missing torch
    #     return [m.__name__ for m in method.META_METHODS if type(m) != str]
    #
    # def get_classification_algorithm_names(self):
    #     return ['MultinomialNB',
    #             'LinearSVC',
    #             'LogisticRegressionCV',
    #             'RandomForestClassifier',
    #             'SVMperf',
    #             'LowRankLogisticRegression',
    #             'LSTMnet',
    #             'CNNnet',
    #             ]
    #
    # def get_calibration_algorithm_names(self):
    #     return [subclass.__name__ for subclass in RecalibratedProbabilisticClassifierBase.__subclasses__()]
    #
    # # TODO allow use of RecalibratedProbabilisticClassifierBase + CalibratorFactory?
    # # def get_calibrator_names(self):
    # #     return [subclass.__name__ for subclass in CalibratorFactory.__subclasses__()]
    #
    # def get_sampling_protocol_names(self):
    #     return ["APP", "NPP", "UPP"]
