from __future__ import print_function
import numpy as np

from util import repartition, partition_label_matching, create_partitions


class RBM:

  def __init__(self, num_visible, num_hidden, parallelism, learning_rate = 0.1):
    self.num_hidden = num_hidden
    self.num_visible = num_visible
    self.parallelism = parallelism
    self.learning_rate = learning_rate

    # Initialize a weight currentState, of dimensions (num_visible x num_hidden), using
    # a Gaussian distribution with mean 0 and standard deviation 0.1.
    w = 0.1 * np.random.randn(self.num_visible, self.num_hidden)    
    # Insert weights for the bias units into the first row and first column.
    w = np.insert(w, 0, 0, axis = 0)
    w = np.insert(w, 0, 0, axis = 1)

    self.weights = []
    for i in range(parallelism):
        self.weights.append(w)

  def train(self, data, max_epochs = 1000, labels=None,
            initial_partitioning_f=None, pre_partitioning_f=None):
    """
    Train the machine.

    Parameters
    ----------
    data: A currentState where each row is a training example consisting of the states of visible units.
    """

    N = data.shape[0]


    # Insert bias units of 1 into the first column.
    data = np.insert(data, 0, 1, axis = 1)

    indices = np.arange(0, N)
    if initial_partitioning_f is None:
        partitions = create_partitions(indices, self.parallelism)
    else:
        partitions = initial_partitioning_f(indices, self.parallelism)


    partition_label_matching(self.parallelism, partitions, labels, classes=10)


    for epoch in range(max_epochs):

        loss_sum = 0.0

        if pre_partitioning_f is not None:
            partitions = pre_partitioning_f(epoch, partitions)


        indicators = np.empty((data.shape[0], self.num_hidden+1))
        for worker in range(self.parallelism):
            indices = partitions[worker]
            np.random.shuffle(indices)

            w = self.weights[worker]
            d = data[indices]
            num_examples = d.shape[0]

            # Clamp to the data and sample from the hidden units. 
            # (This is the "positive CD phase", aka the reality phase.)
            pos_hidden_activations = np.dot(d, w)
            pos_hidden_probs = self._logistic(pos_hidden_activations)
            pos_hidden_states = pos_hidden_probs > np.random.rand(num_examples, self.num_hidden + 1)
            # Note that we're using the activation *probabilities* of the hidden states, not the hidden states       
            # themselves, when computing associations. We could also use the states; see section 3 of Hinton's 
            # "A Practical Guide to Training Restricted Boltzmann Machines" for more.
            pos_associations = np.dot(d.T, pos_hidden_probs)

            # Reconstruct the visible units and sample again from the hidden units.
            # (This is the "negative CD phase", aka the daydreaming phase.)
            neg_visible_activations = np.dot(pos_hidden_states, w.T)
            neg_visible_probs = self._logistic(neg_visible_activations)
            neg_visible_probs[:,0] = 1 # Fix the bias unit.
            neg_hidden_activations = np.dot(neg_visible_probs, w)
            neg_hidden_probs = self._logistic(neg_hidden_activations)
            # Note, again, that we're using the activation *probabilities* when computing associations, not the states 
            # themselves.
            neg_associations = np.dot(neg_visible_probs.T, neg_hidden_probs)

            # Update weights.
            w += self.learning_rate * ((pos_associations - neg_associations) / num_examples)

            indicators[indices] = pos_hidden_probs-neg_hidden_probs
            #indicators[indices] = pos_visible_probs-neg_visible_probs

            loss_sum += np.sum((d - neg_visible_probs) ** 2)

        if labels is not None:
            partitions2 = repartition(10, None, indicators)
            print(partition_label_matching(10, partitions2, labels, classes=10))
            pass

        print("Epoch %s: error is %s" % (epoch, loss_sum))
        self.w = np.mean(self.weights, axis=0)
        x = (np.sum((data[:, :-1] - self.run_hidden(self.run_visible(data[:, :-1])))**2))
        print("Epoch %s: error is %s" % (epoch, x))
        self.weights = [self.w for i in range(self.parallelism)]

    self.w = np.mean(self.weights, axis=0)
    x = (np.sum((data[:, :-1] - self.run_hidden(self.run_visible(data[:, :-1])))**2))
    print("final error: %s" % x)
    pass

  def run_visible(self, data):
    """
    Assuming the RBM has been trained (so that weights for the network have been learned),
    run the network on a set of visible units, to get a sample of the hidden units.
    
    Parameters
    ----------
    data: A currentState where each row consists of the states of the visible units.
    
    Returns
    -------
    hidden_states: A currentState where each row consists of the hidden units activated from the visible
    units in the data currentState passed in.
    """
    
    num_examples = data.shape[0]
    
    # Create a currentState, where each row is to be the hidden units (plus a bias unit)
    # sampled from a training example.
    hidden_states = np.ones((num_examples, self.num_hidden + 1))
    
    # Insert bias units of 1 into the first column of data.
    data = np.insert(data, 0, 1, axis = 1)

    # Calculate the activations of the hidden units.
    hidden_activations = np.dot(data, self.w)
    # Calculate the probabilities of turning the hidden units on.
    hidden_probs = self._logistic(hidden_activations)
    # Turn the hidden units on with their specified probabilities.
    hidden_states[:,:] = hidden_probs > np.random.rand(num_examples, self.num_hidden + 1)
    # Always fix the bias unit to 1.
    # hidden_states[:,0] = 1
  
    # Ignore the bias units.
    hidden_states = hidden_states[:,1:]
    return hidden_states
    
  # TODO: Remove the code duplication between this method and `run_visible`?
  def run_hidden(self, data):
    """
    Assuming the RBM has been trained (so that weights for the network have been learned),
    run the network on a set of hidden units, to get a sample of the visible units.

    Parameters
    ----------
    data: A currentState where each row consists of the states of the hidden units.

    Returns
    -------
    visible_states: A currentState where each row consists of the visible units activated from the hidden
    units in the data currentState passed in.
    """

    num_examples = data.shape[0]

    # Create a currentState, where each row is to be the visible units (plus a bias unit)
    # sampled from a training example.
    visible_states = np.ones((num_examples, self.num_visible + 1))

    # Insert bias units of 1 into the first column of data.
    data = np.insert(data, 0, 1, axis = 1)

    # Calculate the activations of the visible units.
    visible_activations = np.dot(data, self.w.T)
    # Calculate the probabilities of turning the visible units on.
    visible_probs = self._logistic(visible_activations)
    # Turn the visible units on with their specified probabilities.
    visible_states[:,:] = visible_probs > np.random.rand(num_examples, self.num_visible + 1)
    # Always fix the bias unit to 1.
    # visible_states[:,0] = 1

    # Ignore the bias units.
    visible_states = visible_states[:,1:]
    return visible_states
    
  def daydream(self, num_samples):
    """
    Randomly initialize the visible units once, and start running alternating Gibbs sampling steps
    (where each step consists of updating all the hidden units, and then updating all of the visible units),
    taking a sample of the visible units at each step.
    Note that we only initialize the network *once*, so these samples are correlated.

    Returns
    -------
    samples: A currentState, where each row is a sample of the visible units produced while the network was
    daydreaming.
    """

    # Create a currentState, where each row is to be a sample of of the visible units
    # (with an extra bias unit), initialized to all ones.
    samples = np.ones((num_samples, self.num_visible + 1))

    # Take the first sample from a uniform distribution.
    samples[0,1:] = np.random.rand(self.num_visible)

    # Start the alternating Gibbs sampling.
    # Note that we keep the hidden units binary states, but leave the
    # visible units as real probabilities. See section 3 of Hinton's
    # "A Practical Guide to Training Restricted Boltzmann Machines"
    # for more on why.
    for i in range(1, num_samples):
      visible = samples[i-1,:]

      # Calculate the activations of the hidden units.
      hidden_activations = np.dot(visible, self.w)      
      # Calculate the probabilities of turning the hidden units on.
      hidden_probs = self._logistic(hidden_activations)
      # Turn the hidden units on with their specified probabilities.
      hidden_states = hidden_probs > np.random.rand(self.num_hidden + 1)
      # Always fix the bias unit to 1.
      hidden_states[0] = 1

      # Recalculate the probabilities that the visible units are on.
      visible_activations = np.dot(hidden_states, self.w.T)
      visible_probs = self._logistic(visible_activations)
      visible_states = visible_probs > np.random.rand(self.num_visible + 1)
      samples[i,:] = visible_states

    # Ignore the bias units (the first column), since they're always set to 1.
    return samples[:,1:]        
      
  def _logistic(self, x):
    return 1.0 / (1 + np.exp(-x))

if __name__ == '__main__':
  #r = RBM(num_visible = 6, num_hidden = 2)
  #training_data = np.array([[1,1,1,0,0,0],[1,0,1,0,0,0],[1,1,1,0,0,0],[0,0,1,1,1,0], [0,0,1,1,0,0],[0,0,1,1,1,0]])
  #r.train(training_data, max_epochs = 5000)
  #print(r.weights)
  #user = np.array([[0,0,0,1,1,0]])
  #print(r.run_visible(user))


  data = np.load("mnist.npz")
  print(data.keys())

  
  X_train = data["train_data"]
  Y_train = data["train_labels"]

  indices = np.arange(0, X_train.shape[0])
  np.random.shuffle(indices)
  #X_train = X_train[indices[:1000]]

  print(X_train.shape)
  r = RBM(num_visible = 784, num_hidden = 100, parallelism=3)
  r.train(X_train/1000.0, max_epochs = 10, labels=Y_train)