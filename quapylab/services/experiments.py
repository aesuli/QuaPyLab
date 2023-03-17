from quapy.classification.calibration import VSCalibration
from quapy.data import LabelledCollection
from quapy.method.aggregative import EMQ
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegressionCV
from sklearn.preprocessing import LabelEncoder

from quapylab.db.quapydb import QuaPyDB, get_label_column_name, get_text_column_name, get_data_column_names
from quapylab.services.background_processor import job_function

try:
    from quapy.classification.neural import LSTMnet, CNNnet
except ModuleNotFoundError:
    LSTMnet = "Torch is not installed"
    CNNnet = "Torch is not installed"


@job_function
def train_quantifier(db: QuaPyDB, job_id, name, overwrite=False):
    df = db.get_dataset(name)

    label_column_name = get_label_column_name(df)

    y = df[label_column_name].to_list()

    # TODO I need to encode labels due to
    #  RecalibratedProbabilisticClassifierBase.fit_cv, calibration.py, line 79
    #  Should it be changed to work with string labels?
    y = LabelEncoder().fit_transform(y)
    # TODO currently not saving the label encoder. I'm waiting to see if it is required or not.

    text_column_name = get_text_column_name(df)

    if text_column_name is not None:
        X = df[text_column_name]

        vectorizer = TfidfVectorizer()
        X = vectorizer.fit_transform(X)
    else:
        data_column_names = get_data_column_names(df)
        X = df[data_column_names].to_numpy()

    training_data = LabelledCollection(X, y)

    # TODO Comparison of multiple models

    # TODO Report on the selection process and performance measures

    quantifier = EMQ(VSCalibration(LogisticRegressionCV()))

    quantifier.fit(training_data)

    db.set_quantifier(name, quantifier, overwrite)
