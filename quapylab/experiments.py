import logging
import traceback

import quapy.data.datasets
from quapy.classification.calibration import NBVSCalibration, BCTSCalibration, TSCalibration, VSCalibration
from quapy.classification.methods import LowRankLogisticRegression
from quapy.method.meta import Ensemble
from quapy.model_selection import GridSearchQ
from quapy.protocol import NPP, APP, UPP
from sklearn.naive_bayes import MultinomialNB

try:
    from quapy.classification.neural import LSTMnet, CNNnet
except ModuleNotFoundError:
    LSTMnet = "Torch is not installed"
    CNNnet = "Torch is not installed"

from quapy.classification.svmperf import SVMperf
from quapy.method.aggregative import SMM, MS2, PACC, DyS, T50, MS, ACC, X, MAX, HDy, EMQ, PCC, CC
from quapy.method.non_aggregative import MaximumLikelihoodPrevalenceEstimation
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegressionCV
from sklearn.svm import LinearSVC

SKIP_NAME = 'None'


def train(**kwargs):
    # TODO sample size as parameter?
    if quapy.environ['SAMPLE_SIZE'] is None:
        quapy.environ['SAMPLE_SIZE'] = 100
    db = kwargs['db']

    dataset_names = kwargs['dataset']
    if type(dataset_names) == str:
        dataset_names = [dataset_names]

    quantifier_names = kwargs['quantifier']
    if type(quantifier_names) == str:
        quantifier_names = [quantifier_names]

    classifier_names = kwargs.get('classifier', [])
    if type(classifier_names) == str:
        classifier_names = [classifier_names]

    calibration_names = kwargs['calibration']
    if type(calibration_names) == str:
        calibration_names = [calibration_names]

    metaquantifier_names = kwargs['metaquatifier']
    if type(metaquantifier_names) == str:
        metaquantifier_names = [metaquantifier_names]

    selection_names = kwargs['selection']
    if type(selection_names) == str:
        selection_names = [selection_names]

    protocol_names = kwargs.get('protocol', [])
    if type(protocol_names) == str:
        protocol_names = [protocol_names]

    name = kwargs['name']

    overwrite = 'overwrite' in kwargs

    for dataset_name, dataset in generate_datasets(dataset_names):
        for quantifier_name, quantifier in generate_quantifiers(dataset, quantifier_names, classifier_names,
                                                                calibration_names, metaquantifier_names,
                                                                selection_names, protocol_names):
            logging.info(f'Fitting experiment {name}, dataset {dataset_name}, quantifier {quantifier_name}')
            try:
                if hasattr(dataset, 'Xy'):
                    quantifier.fit(dataset)
                elif hasattr(dataset, 'training'):
                    quantifier.fit(dataset.training)
                else:
                    raise ValueError(f'Not able to determine how to access training data on dataset "{dataset_name}"')
            except Exception as e:
                # TODO manage errors from unsupported configurations or from failed computation
                logging.exception(f'Error while fitting {name}, dataset {dataset_name}, quantifier {quantifier_name}')
                with open(db._model_dir / name / f'd-{dataset_name}_{quantifier_name}.error.txt', mode='wt',
                          encoding='utf-8') as outputfile:
                    outputfile.write(f'{e}\n{traceback.format_exc()}')
            else:
                logging.info(
                    f'Saving fit model for experiment {name}, dataset {dataset_name}, quantifier {quantifier_name}')
                db.set_quantifier(quantifier, name, f'd-{dataset_name}_{quantifier_name}', overwrite=overwrite)


def generate_datasets(dataset_names):
    for dataset_fullname in dataset_names:
        group_name, dataset_name = dataset_fullname.split(':')
        if group_name == 'Reviews sentiment':
            # TODO custom indexing?
            yield f'{group_name}_{dataset_name}', quapy.data.datasets.fetch_reviews(
                dataset_name, tfidf=True)  # ,min_df=,data_home=,pickle=)
        elif group_name == 'Twitter train':
            yield f'{group_name}_{dataset_name}', quapy.datasets.fetch_twitter(dataset_name,
                                                                               for_model_selection=True)  # ,min_df=,data_home=,pickle=)
        elif group_name == 'Twitter test':
            yield f'{group_name}_{dataset_name}', quapy.datasets.fetch_twitter(dataset_name,
                                                                               for_model_selection=False)  # ,min_df=,data_home=,pickle=)
        elif group_name == 'UCI':
            yield f'{group_name}_{dataset_name}', quapy.datasets.fetch_UCIDataset(
                dataset_name)  # ,data_home=,test_split=,verbose=)
            # TODO should use the one used above or fetch_UCILabelledCollection?
            #  or allow users to use both by letting them specifying which one?
        elif group_name == 'LeQua 2022':
            # TODO Is it correct to return only the first element of the tuple?
            yield f'{group_name}_{dataset_name}', quapy.datasets.fetch_lequa2022(dataset_name)[0]  # ,data_home=)
        elif group_name == 'User data':
            raise NotImplemented('not yet implemented')
            # TODO define how user data is uploaded and then how it is loaded here
            # yield quapy.datasets.from_text(dataset_name)#,encoding=,verbose=,class2int=)
            # yield quapy.datasets.from_sparse(dataset_name)
            # yield quapy.datasets.from_csv(dataset_name)#,encoding=)
        else:
            raise ValueError(f'Unknown dataset: {dataset_fullname}')


def generate_quantifiers(dataset, quantifier_names, classifier_names, calibration_names, metaquantifier_names,
                         selection_names, protocol_names):
    for metaquantifier_name, selection_name, protocol_name, meta_wrapper, selection_wrapper in generate_wrappers(
            dataset, metaquantifier_names, selection_names, protocol_names):
        for quantifier_name in quantifier_names:
            if quantifier_name == 'MaximumLikelihoodPrevalenceEstimation':
                yield f'{selection_name}_{protocol_name}_{quantifier_name}', MaximumLikelihoodPrevalenceEstimation()
            else:
                for classifier_name, classifier in generate_classifiers(classifier_names, calibration_names):
                    if quantifier_name == 'SMM':
                        quantifier = SMM(classifier)  # ,val_split=)
                    elif quantifier_name == 'MS2':
                        quantifier = MS2(classifier)  # ,val_split=)
                    elif quantifier_name == 'PACC':
                        quantifier = PACC(classifier)  # ,val_split=,n_jobs=)
                    elif quantifier_name == 'DyS':
                        quantifier = DyS(classifier)  # ,val_split=,n_bins=,divergence=,tol=)
                    elif quantifier_name == 'T50':
                        quantifier = T50(classifier)  # ,val_split=)
                    elif quantifier_name == 'MS':
                        quantifier = MS(classifier)  # ,val_split=)
                    elif quantifier_name == 'ACC':
                        quantifier = ACC(classifier)  # ,val_split=,n_jobs=)
                    elif quantifier_name == 'X':
                        quantifier = X(classifier)  # ,val_split=)
                    elif quantifier_name == 'MAX':
                        quantifier = MAX(classifier)  # ,val_split=)
                    elif quantifier_name == 'HDy':
                        quantifier = HDy(classifier)  # ,val_split=)
                    elif quantifier_name == 'EMQ':
                        quantifier = EMQ(classifier)  # ,exact_train_prev=,recalib=)
                    elif quantifier_name == 'PCC':
                        quantifier = PCC(classifier)
                    elif quantifier_name == 'CC':
                        quantifier = CC(classifier)
                    else:
                        raise ValueError(f'Unknown quantifier: {quantifier_name}')

                    # TODO some configurations are not supported e.g., GridSearchQ(Ensemble(Q))
                    #  how should we handle them?
                    try:
                        quantifier = meta_wrapper(quantifier)
                        quantifier = selection_wrapper(quantifier)
                    except Exception as e:
                        quantifier = e
                    yield f'sel-{selection_name}-{protocol_name}_meta-{metaquantifier_name}_q-{quantifier_name}_c-{classifier_name}', quantifier
                # TODO what about OneVsAllAggregative?
                #  are there other relevant quantifiers to be added here?


def generate_wrappers(dataset, metaquantifier_names, selection_names, protocol_names):
    for metaquantifier_name in metaquantifier_names:
        if metaquantifier_name == SKIP_NAME:
            meta_wrapper = lambda x: x
        elif metaquantifier_name == "Ensemble":
            meta_wrapper = Ensemble
        else:
            raise ValueError(f'Unknown meta quantification method: {metaquantifier_name}')
        for selection_name in selection_names:
            if selection_name == SKIP_NAME:
                yield metaquantifier_name, selection_name, SKIP_NAME, meta_wrapper, lambda quantifier: quantifier
            elif selection_name == 'Grid':
                for protocol_name in protocol_names:
                    # TODO parameters of the sampling protocol
                    if protocol_name == 'APP':
                        protocol = APP
                    elif protocol_name == 'NPP':
                        protocol = NPP
                    elif protocol_name == 'UPP':
                        protocol = UPP
                    else:
                        raise ValueError(f'Unknown sampling protocol: {protocol_name}')

                    # TODO parameters of the model selection method
                    # TODO set measure to optimize
                    selection_wrapper = lambda quantifier: GridSearchQ(quantifier, {k: [v] for k, v in
                                                                                    quantifier.get_params().items()},
                                                                       protocol(dataset.training))

                    yield metaquantifier_name, selection_name, protocol_name, meta_wrapper, selection_wrapper
            else:
                raise ValueError(f'Unknown model selection method: {selection_name}')


def generate_classifiers(classifier_names, calibration_names):
    # TODO changing default parameters for each algorithm
    # TODO classification-oriented optimization of parameters?
    for classifier_name in classifier_names:
        if classifier_name == 'MultinomialNB':
            classifier = MultinomialNB()
        elif classifier_name == "LinearSVC":
            classifier = LinearSVC()
        elif classifier_name == "LogisticRegressionCV":
            classifier = LogisticRegressionCV()
        elif classifier_name == "RandomForestClassifier":
            classifier = RandomForestClassifier()
        elif classifier_name == "SVMperf":
            classifier = SVMperf(quapy.environ['SVMPERF_HOME'])
        elif classifier_name == "LowRankLogisticRegression":
            classifier = LowRankLogisticRegression()
        elif classifier_name == "LSTMnet":
            if type(LSTMnet) == str:
                classifier = None
            else:
                # TODO required parameters of LSTMnet
                classifier = LSTMnet()
        elif classifier_name == "CNNnet":
            if type(CNNnet) == str:
                classifier = None
            else:
                # TODO required parameters of CNNnet
                classifier = CNNnet()
        else:
            raise ValueError(f'Unknown classifier: {classifier_name}')

        for calibration_name in calibration_names:
            if calibration_name == "NBVSCalibration":
                classifier = NBVSCalibration(classifier)  # ,val_split=,n_jobs=,verbose=)
            elif calibration_name == "BCTSCalibration":
                classifier = BCTSCalibration(classifier)  # ,val_split=,n_jobs=,verbose=)
            elif calibration_name == "TSCalibration":
                classifier = TSCalibration(classifier)  # ,val_split=,n_jobs=,verbose=)
            elif calibration_name == "VSCalibration":
                classifier = VSCalibration(classifier)  # ,val_split=,n_jobs=,verbose=)
            elif calibration_name != SKIP_NAME:
                raise ValueError(f'Unknown calibration: {calibration_name}')

            yield f'{classifier_name}_{calibration_name}', classifier
