import numpy
from sklearn.datasets import load_svmlight_file

RCV1_TRAIN_SET_PATH = "/media/alber/datadisk/packages/datasets/rcv1/rcv1_train.binary"
#RCV1_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/rcv1/rcv1_test.binary"
RCV1_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/rcv1/rcv1_test.binary.small"

DOROTHEA_TRAIN_SET_PATH = "/media/alber/datadisk/packages/datasets/dorothea/dorothea_train.svm_light"
DOROTHEA_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/dorothea/dorothea_valid.svm_light"

YOUTUBE_TEXT_DESC_TRAIN_SET_PATH = "/media/alber/datadisk/packages/datasets/youtube_multiview_uci/dir_data/train/text_description_unigrams.txt"
YOUTUBE_TEXT_DESC_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/youtube_multiview_uci/dir_data/validation/text_description_unigrams.txt"


def fetch_dataset(data_set_name, params):
    X_train, Y_train, X_test, Y_test = None, None, None, None

    if data_set_name.startswith("rcv1"):
        X_train, Y_train = load_svmlight_file(RCV1_TRAIN_SET_PATH)
        X_test, Y_test = load_svmlight_file(RCV1_TEST_SET_PATH)
    if data_set_name.startswith("20_news"):
        assert False
        from sklearn.datasets import fetch_20newsgroups
        return fetch_20newsgroups()
    if data_set_name.startswith("dorothea"):
        X_train, Y_train = load_svmlight_file(DOROTHEA_TRAIN_SET_PATH)
        X_test, Y_test = load_svmlight_file(DOROTHEA_TEST_SET_PATH)
    if data_set_name.startswith("youtube_text_desc"):
        n_features = 12183626
        X_train, Y_train = load_svmlight_file(YOUTUBE_TEXT_DESC_TRAIN_SET_PATH,
                                              n_features=n_features)
        X_test, Y_test = load_svmlight_file(YOUTUBE_TEXT_DESC_TEST_SET_PATH,
                                            n_features=n_features)
        Y_train[Y_train != 1] = -1
        Y_test[Y_test != 1] = -1


    assert (
        X_train is not None and
        Y_train is not None and
        X_test is not None and
        Y_test is not None)

    if data_set_name.endswith("noised"):
        assert False  # todo

    if data_set_name.find("limited") > 0:
        N = X_train.shape[0]
        n = params["DS_LIMITED_SIZE"]
        indices = numpy.arange(0, N)
        numpy.random.shuffle(indices)
        X_train = X_train[indices[:n]]
        Y_train = Y_train[indices[:n]]
    return X_train, Y_train, X_test, Y_test
