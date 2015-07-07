import numpy

from scipy.sparse import *

from losses import LOSSES
from util import (
    create_partitions, repartition,
    partition_label_matching)


def run_scikit(X_train, Y_train, X_test, loss, params):
    from sklearn.linear_model import SGDClassifier
    clf = SGDClassifier(loss=loss, penalty=None,
                        alpha=0.0, l1_ratio=0.0,
                        fit_intercept=False,
                        n_iter=params["ITERATIONS"],
                        learning_rate="constant",
                        eta0=params["LEARNING_RATE"],
                        power_t=0.0,
                        epsilon=0.0,
                        shuffle=True,
                        verbose=10)
    clf.fit(X_train, Y_train)

    return clf.predict(X_test)


def run_seq(X_train, Y_train, X_test, loss, params):
    epochs = params["ITERATIONS"]

    N, M = X_train.shape

    learning_rate = params["LEARNING_RATE"]

    w = numpy.random.rand(M)
    b = 0.0

    pred, loss, grad = LOSSES[loss]

    for epoch in range(epochs):
        indices = numpy.arange(0, N)
        numpy.random.shuffle(indices)

        loss_sum = 0.0
        for i in indices:
            #x = numpy.array(X_train[i].todense()).reshape((M,))
            x = X_train[i]
            y = Y_train[i]
            g = grad(w, x, y)
            w = w - learning_rate * g
            loss_sum += loss(w, x, y)
        print "epoch ", epoch, "loss: ", loss_sum/N

    return pred(w, X_test)


def run_para_default(X_train, Y_train, X_test, loss, params):
    epochs = params["ITERATIONS"]

    N, M = X_train.shape

    learning_rate = params["LEARNING_RATE"]
    parallelism = params["PARALLISM"]
    weight_sharing = params["WEIGHT_SHARING"]

    w = None
    w_all = [numpy.random.rand(M) for i in range(parallelism)]
    b = 0.0

    pred, loss, grad = LOSSES[loss]

    indices = numpy.arange(0, N)
    partitions = create_partitions(indices, parallelism)

    if params["PARTITION_SHOW_DIST"]:
        print partition_label_matching(parallelism, partitions, Y_train)

    for epoch in range(epochs):
        loss_sum = 0.0
        for worker in range(parallelism):
            indices = partitions[worker]
            numpy.random.shuffle(indices)

            w_ = w_all[worker]
            for i in indices:
                #x = numpy.array(X_train[i].todense()).reshape((M,))
                x = X_train[i]
                y = Y_train[i]
                g = grad(w_, x, y)
                w_ = w_ - learning_rate * g
                loss_sum += loss(w_, x, y)
            w_all[worker] = w_
        w = numpy.mean(w_all, axis=0)
        if weight_sharing:
            w_all = [w for x in w_all]
        print "epoch ", epoch, "loss: ", loss_sum/N

    return pred(w, X_test)


def run_para_kmeans(X_train, Y_train, X_test, loss, params):
    epochs = params["ITERATIONS"]

    N, M = X_train.shape

    learning_rate = params["LEARNING_RATE"]
    parallelism = params["PARALLISM"]
    weight_sharing = params["WEIGHT_SHARING"]

    w = None
    w_all = [numpy.random.rand(M) for i in range(parallelism)]
    b = 0.0

    pred, loss, grad = LOSSES[loss]

    indices = numpy.arange(0, N)
    partitions = create_partitions(indices, parallelism)

    for epoch in range(epochs):
        loss_sum = 0.0
        gradients = lil_matrix(X_train.shape, dtype=X_train.dtype)

        for worker in range(parallelism):
            indices = partitions[worker]
            numpy.random.shuffle(indices)

            w_ = w_all[worker]
            for i in indices:
                #x = numpy.array(X_train[i].todense()).reshape((M,))
                x = X_train[i]
                y = Y_train[i]
                g = grad(w_, x, y)
                w_ = w_ - learning_rate * g
                loss_sum += loss(w_, x, y)
                gradients[i] = g
            w_all[worker] = w_
        w = numpy.mean(w_all, axis=0)
        if weight_sharing:
            w_all = [w for x in w_all]
        print "epoch ", epoch, "loss: ", loss_sum/N

        # update partitions
        if params["REPARTITION"] == "kmeans":
            partitions2 = repartition(parallelism, partitions, gradients)
        if params["PARTITION_SHOW_DIST"]:
            print partition_label_matching(parallelism, partitions2, Y_train)
        pass

    return pred(w, X_test)


def run_para_label(X_train, Y_train, X_test, loss, params):
    epochs = params["ITERATIONS"]

    N, M = X_train.shape

    learning_rate = params["LEARNING_RATE"]
    parallelism = 2  # fixed to label count
    weight_sharing = params["WEIGHT_SHARING"]

    w = None
    w_all = [numpy.random.rand(M) for i in range(parallelism)]
    b = 0.0

    pred, loss, grad = LOSSES[loss]

    indices = numpy.arange(0, N)
    partitions = [indices[Y_train == -1],
                  indices[Y_train == +1]]

    if params["PARTITION_SHOW_DIST"]:
        print partition_label_matching(parallelism, partitions, Y_train)

    for epoch in range(epochs):
        loss_sum = 0.0

        for worker in range(parallelism):
            indices = partitions[worker]
            numpy.random.shuffle(indices)

            w_ = w_all[worker]
            for i in indices:
                #x = numpy.array(X_train[i].todense()).reshape((M,))
                x = X_train[i]
                y = Y_train[i]
                g = grad(w_, x, y)
                w_ = w_ - learning_rate * g
                loss_sum += loss(w_, x, y)
            w_all[worker] = w_
        w = numpy.mean(w_all, axis=0)
        if weight_sharing:
            w_all = [w for x in w_all]
        print "epoch ", epoch, "loss: ", loss_sum/N
        pass

    return pred(w, X_test)

ALGOS = {
    "scikit": run_scikit,
    "seq": run_seq,
    "para_default": run_para_default,  # fixed parititions no clustering
    "para_kmeans": run_para_kmeans,  # cluster paritions with kmeans
    "para_label": run_para_label,  # preproc: cluster according to label
}
