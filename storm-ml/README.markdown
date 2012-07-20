Online machine learning library on top of Storm

## Usage 

The perceptron topology is located at https://github.com/git2samus/storm-contrib/blob/ml-perceptron/storm-ml/

The class [storm.ml.PerceptronDRPCTopology](https://github.com/git2samus/storm-contrib/blob/ml-perceptron/storm-ml/src/jvm/storm/ml/PerceptronDRPCTopology.java) serves as a template for your particular implementation, currently the perceptron topology consists of two Storm topologies but in the future they'll be a single one wrapped on a specialized builder.

In order to use this topology you need to implement a spout that subclasses [storm.ml.spout.BaseTrainingSpout](https://github.com/git2samus/storm-contrib/blob/ml-perceptron/storm-ml/src/jvm/storm/ml/spout/BaseTrainingSpout.java) for feeding your training data ([storm.ml.spout.TrainingSpout](https://github.com/git2samus/storm-contrib/blob/ml-perceptron/storm-ml/src/jvm/storm/ml/spout/TrainingSpout.java) is an example) the rest of the template remains the same.

You'll then have a DRPC implementation for the command "evaluate" which takes as a paramenter the string representation of a `Double[]` datatype, which answers your request with the evaluated result.

The current implementation requires a memcached instance/cluster to be running, it'll write on the key "weights" and you can download and install all the dependencies by running `lein deps` from [leiningen](https://github.com/technomancy/leiningen).