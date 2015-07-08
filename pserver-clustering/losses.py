import numpy

LARGE_NUM = 10**2


def clip(tmp):
    tmp[tmp > LARGE_NUM] = LARGE_NUM
    tmp[tmp < -LARGE_NUM] = -LARGE_NUM
    return tmp


def log_reg_pred(w, X):
    N, M = X.shape
    Y = 1.0/(1+numpy.exp(clip(X.dot(w.T))))
    return (Y < 0.5)*2-1


def log_reg_loss(w, x, y):
    return numpy.log(1+numpy.exp(clip(-y * x.dot(w.T))))


def log_reg_grad(w, x, y):
    if isinstance(x, numpy.ndarray):
        return x * (-y * (1 - 1/(1+numpy.exp(clip(-y * x.dot(w.T))))))
    else:
        return x.multiply(-y * (1 - 1/(1+numpy.exp(clip(-y * x.dot(w.T))))))


LOSSES = {
    "log": (log_reg_pred, log_reg_loss, log_reg_grad)
}
