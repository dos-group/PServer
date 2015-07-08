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


def run_para_default(X_train, Y_train, X_test, loss, params,
                     initial_partitioning_f=None,
                     pre_partitioning_f=None,
                     post_partitioning_f=None,
                     store_gradients=False):
    epochs = params["ITERATIONS"]

    N, M = X_train.shape

    learning_rate = params["LEARNING_RATE"]
    parallelism = params["PARALLELISM"]
    weight_sharing = params["WEIGHT_SHARING"]

    w = None
    w_all = [numpy.random.rand(M) for i in range(parallelism)]
    b = 0.0

    pred, loss, grad = LOSSES[loss]

    indices = numpy.arange(0, N)
    if initial_partitioning_f is None:
        partitions = create_partitions(indices, parallelism)
    else:
        partitions = initial_partitioning_f(indices, parallelism)

    if params["PARTITION_SHOW_DIST"]:
        print partition_label_matching(parallelism, partitions, Y_train)

    for epoch in range(epochs):
        loss_sum = 0.0

        if pre_partitioning_f is not None:
            partitions = pre_partitioning_f(epoch, partitions)

        gradients = None
        if store_gradients:
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
                if gradients is not None:
                    gradients[i] = g
            w_all[worker] = w_
        w = numpy.mean(w_all, axis=0)
        if weight_sharing:
            w_all = [w for x in w_all]
        print "epoch ", epoch, "loss: ", loss_sum/N

        if post_partitioning_f is not None:
            partitions = post_partitioning_f(epoch,
                                             partitions,
                                             gradients)

    return pred(w, X_test)


def run_para_label(X_train, Y_train, X_test, loss, params):
    params["PARALLELISM"] = 2  # we just have 2 class problems

    def initial_partitioning_f(indices, parallelism):
        partitions = [indices[Y_train == -1],
                      indices[Y_train == +1]]
        return partitions

    return run_para_default(X_train, Y_train, X_test, loss, params,
                            initial_partitioning_f=initial_partitioning_f)


def run_para_kmeans(X_train, Y_train, X_test, loss, params):
    parallelism = params["PARALLELISM"]

    def post_partitioning_f(epoch, partitions, gradients):
        # update partitions
        if params["REPARTITION"] == "kmeans":
            partitions2 = repartition(parallelism, partitions, gradients)
        if params["PARTITION_SHOW_DIST"]:
            print partition_label_matching(parallelism, partitions2, Y_train)
        return partitions
    return run_para_default(X_train, Y_train, X_test, loss, params,
                            post_partitioning_f=post_partitioning_f,
                            store_gradients=True)


def run_para_abskmeans(X_train, Y_train, X_test, loss, params):
    parallelism = params["PARALLELISM"]

    def post_partitioning_f(epoch, partitions, gradients):
        # update partitions
        if params["REPARTITION"] == "kmeans":
            partitions2 = repartition(parallelism, partitions,
                                      numpy.abs(gradients))
        if params["PARTITION_SHOW_DIST"]:
            print partition_label_matching(parallelism, partitions2, Y_train)
        return partitions
    return run_para_default(X_train, Y_train, X_test, loss, params,
                            post_partitioning_f=post_partitioning_f,
                            store_gradients=True)


ALGOS = {
    "scikit": run_scikit,
    "seq": run_seq,
    "para_default": run_para_default,  # fixed parititions no clustering
    "para_label": run_para_label,  # preproc: cluster according to label
    # cluster paritions with kmeans according to gradient
    "para_kmeans": run_para_kmeans,
    # cluster paritions with kmeans according to absolute gradient
    "para_abskmeans": run_para_abskmeans,
}
