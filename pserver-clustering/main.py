import sys
import time

from sklearn.metrics import *

from algos import ALGOS
from datasets import fetch_dataset


# parameters shared between all algorithms
GLOBAL_PARAMS = {
    # Epoch count:
    "ITERATIONS": 5,
    # SGD learning rate:
    "LEARNING_RATE": 0.4,
    # Parallelism of parallel SGD:
    "PARALLELISM": 4,
    # Share weights after each epoch in parallel SGD:
    "WEIGHT_SHARING": True,

    # Print stats on repartitioning
    "PARTITION_SHOW_DIST": True,
    # Repartition after first epoch
    "REPARTITION": "kmeans",

    # size of "limited" data sets
    "DS_LIMITED_SIZE": 300,
}


def run_algo(algo, data, loss):
    X_train, Y_train, X_test, Y_test = data

    algo_f = ALGOS[algo]
    Y_pred = algo_f(X_train, Y_train, X_test, loss, GLOBAL_PARAMS.copy())

    print classification_report(Y_test, Y_pred)
    print confusion_matrix(Y_test, Y_pred)
    print accuracy_score(Y_test, Y_pred)
    pass

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "usage: data_set_name loss_name algo1 algo2 algo3"
    data = fetch_dataset(sys.argv[1], GLOBAL_PARAMS.copy())
    loss = sys.argv[2]

    for algo in sys.argv[3:]:
        print "=================> start", algo
        start_time = time.time()
        run_algo(algo, data, loss)
        stop_time = time.time()
        print "=================> stopped", algo,
        print " time: ", (stop_time-start_time)
    pass
