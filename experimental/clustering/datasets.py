import numpy
from sklearn.datasets import load_svmlight_file

from util import create_partitions

RCV1_TRAIN_SET_PATH = "/media/alber/datadisk/packages/datasets/rcv1/rcv1_train.binary"
#RCV1_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/rcv1/rcv1_test.binary"
RCV1_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/rcv1/rcv1_test.binary.small"

DOROTHEA_TRAIN_SET_PATH = "/media/alber/datadisk/packages/datasets/dorothea/dorothea_train.svm_light"
DOROTHEA_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/dorothea/dorothea_valid.svm_light"

YOUTUBE_TRAIN_SET_PATH = "/media/alber/datadisk/packages/datasets/youtube_multiview_uci/dir_data/train/%s.txt"
YOUTUBE_TEST_SET_PATH = "/media/alber/datadisk/packages/datasets/youtube_multiview_uci/dir_data/validation/%s.txt"


# youtube sets: valid ones are:
youtube_features = {
    #"audio_mfcc": None,
    "text_description_unigrams": 12183626,
    "text_game_lda_1000": None,
    "text_tag_unigrams": None,
    "vision_hist_motion_estimate": None,
}


def load_youtube_set(stateName, n_features=None, class_=1):
    X_train, Y_train = load_svmlight_file(YOUTUBE_TRAIN_SET_PATH % stateName,
                                          n_features=n_features)
    X_test, Y_test = load_svmlight_file(YOUTUBE_TEST_SET_PATH % stateName,
                                        n_features=n_features)
    Y_train[Y_train != class_] = -1
    Y_test[Y_test != class_] = -1
    return X_train, Y_train, X_test, Y_test


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
    if data_set_name.startswith("youtube"):
        stateName = data_set_name[len("youtube_"):]
        X_train, Y_train, X_test, Y_test = load_youtube_set(
            stateName,
            youtube_features[stateName])
        print X_train.shape[1], "features"

    if data_set_name.startswith("mv_gaussian"):
        N_train = params["MV_GAUSSIAN_TRAIN_SAMPLES"]
        N_test = params["MV_GAUSSIAN_TEST_SAMPLES"]
        N = N_train + N_test
        M = params["MV_GAUSSIAN_DIMENSION"]
        means = [numpy.random.rand(M), numpy.random.rand(M)]
        covs = [numpy.random.rand(M, M), numpy.random.rand(M, M)]
        labels = [-1, +1]
        Ns = [N/2, N-N/2]

        sample_arrs = []
        label_arrs = []
        for mean, cov, label, n in zip(means, covs, labels, Ns):
            sample_arrs.append(
                numpy.random.multivariate_normal(mean, cov, n))
            label_arrs.append(numpy.ones((n, 1)) * label)

        samples = numpy.vstack(sample_arrs)
        labels = numpy.vstack(label_arrs)

        print N, samples.shape, labels.shape
        indices = numpy.arange(N)
        numpy.random.shuffle(indices)
        samples = samples[indices]
        labels = labels[indices]

        X_train = samples[:N_train]
        Y_train = labels[:N_train]
        X_test = samples[N_train:]
        Y_test = labels[N_train:]

    if data_set_name.startswith("mv_gaussian_sliced"):
        M = params["MV_GAUSSIAN_DIMENSION"]
        S = params["MV_GAUSSIAN_SLICES"]

        slices = create_partitions(numpy.arange(M), S)
        for X, Y in [(X_train, Y_train), (X_test, Y_test)]:
            for i in range(X.shape[0]):
                if Y[i] == -1:
                    slice = slices[numpy.random.randint(0, S)]
                    x = numpy.zeros(M)
                    x[slice] = X[i, slice]
                    X[i] = x
        pass

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
