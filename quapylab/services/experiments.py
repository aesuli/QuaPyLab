import quapy as qp
from quapy.classification.calibration import VSCalibration
from quapy.data import LabelledCollection
from quapy.method.aggregative import EMQ, PACC, CC
from quapy.protocol import APP
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegressionCV
from sklearn.preprocessing import LabelEncoder
from sklearn.svm import LinearSVC

from quapylab.db.quapydb import QuaPyDB, get_label_column_name, get_text_column_name, get_data_column_names
from quapylab.services.background_processor import job_function

try:
    from quapy.classification.neural import LSTMnet, CNNnet
except ModuleNotFoundError:
    LSTMnet = "Torch is not installed"
    CNNnet = "Torch is not installed"

# TODO is it corrects to have this here?
qp.environ["SAMPLE_SIZE"] = 100


@job_function
def train_quantifier(db: QuaPyDB, job_id, name, overwrite=False, verbose=True):
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

    all_data = LabelledCollection(X, y)

    train, test = all_data.split_stratified(train_prop=0.75)

    def models():
        yield 'CC_SVM', CC(LinearSVC())
        # yield 'ACC_SVM', ACC(LinearSVC())
        # yield 'PCC_SVM', PCC(LinearSVC())
        yield 'PACC_SVM', PACC(LinearSVC())
        yield 'EMQ_SVM', EMQ(LinearSVC())
        # yield 'CC_LR', CC(VSCalibration(LogisticRegressionCV()))
        # yield 'ACC_LR', ACC(VSCalibration(LogisticRegressionCV()))
        # yield 'PCC_LR', PCC(VSCalibration(LogisticRegressionCV()))
        # yield 'PACC_LR', PACC(VSCalibration(LogisticRegressionCV()))
        yield 'EMQ_LR', EMQ(VSCalibration(LogisticRegressionCV()))

    quantifiers, method_names, true_prevs, estim_prevs, tr_prevs = [], [], [], [], []

    for method_name, model in models():
        model.fit(train)
        quantifiers.append(model)
        true_prev, estim_prev = qp.evaluation.prediction(model, APP(test, repeats=100, random_state=0))

        method_names.append(method_name)
        true_prevs.append(true_prev)
        estim_prevs.append(estim_prev)
        tr_prevs.append(train.prevalence())

    qp.plot.binary_diagonal(method_names, true_prevs, estim_prevs, train_prev=tr_prevs[0],
                            savepath=db.get_report_dir() / f'{name}_bin_diag.png')

    qp.plot.binary_bias_global(method_names, true_prevs, estim_prevs,
                               savepath=db.get_report_dir() / f'{name}_bin_bias.png')

    qp.plot.error_by_drift(method_names, true_prevs, estim_prevs, tr_prevs,
                           error_name='ae', n_bins=10, savepath=db.get_report_dir() / f'{name}_err_drift.png')

    best_i = -1
    best_score = float('inf')
    scores = []
    for i, (method_name, true_prev, estim_prev) in enumerate(zip(method_names, true_prevs, estim_prevs)):
        scores.append([qp.error.mrae(true_prev, estim_prev), qp.error.mae(true_prev, estim_prev),
                       qp.error.mkld(true_prev, estim_prev)])
        if scores[i][0] < best_score:
            best_i = i
            best_score = scores[i][0]

    db.set_quantifier(name, quantifiers[best_i], overwrite)

    with open(db.get_report_dir() / f'{name}_report.html', mode='tw', encoding='utf-8') as outputfile:
        print('<table>', file=outputfile)
        print('<thead><tr><th>Dataset</th><th>Method</th><th>Best</th><th>MRAE</th><th>MAE</th><th>MKLD</th></tr></thead>',
              file=outputfile)
        print('<tbody>', file=outputfile)
        for i, (method_name, true_prev, estim_prev, (mrae, mae, mkld)) in enumerate(
                zip(method_names, true_prevs, estim_prevs, scores)):
            print(
                f'<tr><td>{name}</td><td>{method_name}</td><td>{"*" if i == best_i else ""}</td><td>{mrae:.3g}</td><td>{mae:.3g}</td><td>{mkld:.3g}</td></tr>',
                file=outputfile)
        print('<tbody>', file=outputfile)
        print('<tfoot></tfoot>', file=outputfile)
