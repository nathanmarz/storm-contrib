# Tutorial

## Intent

This tutorial is intended to set you started on using our pre-built topologies or  developing your own Topologies for doing Machine Learning tasks.
For this tutorial you don't need to have a background in Machine Learning but basic knowledge of Storm is required.

## Basic concepts

Many tasks in Machine Learning require big amounts of processing power, and sometimes, there are problems that can be solved online, and by online we mean that can be solved without recalculating the whole training set. To train our algorithm we need to train it and for that we need to plug it to a source (spout) of data that provides examples over time. You can see us work with example spouts in this tutorial.

Then, what we need, is to define how to use that training set to be able to predict new points, for that we have to select an algorithm. The selection of the algorithms is knit together with the nature of your data, and most of the times you need some previous analysis over your training set to see which algorithm to choose. Think of at least plotting your algorithm.

## Infrastructure

First of all one thing to note is that : 
The current implementation requires a memcached instance/cluster to be running. Also you need maven.

In Mac OSX you can do:

    brew install memcached

And then follow the instruction to launch it using launchctl.

Basically and conceptually what you get from our project are two pipes (topologies).

One of these pipes is the "training" pipe, training examples should go through here. The good thing about training this way is that if examples are coming your way very quickly there is potentially infinite power you can give to this pipe.

The other pipe is the "evaluation" pipe. Use this pipe to classify new pieces of data. This is a DRPC implementation for the command "evaluate" which takes as a paramenter the string representation of a `Double[]` datatype, which answers your request with the evaluated result.

## Algorithms

### Perceptron

The perceptron is a parametric algorithm that is useful in cases when data is linearly separable.

### Logistic Regression

## Running the most basic example

### Example run

You can use the example topology in com.twitter.storm.example

### Preparing for training 

We provide a class called MLTopologyBuilder that you can use to quickly build your learning topology. 

As we mentioned before in order to use this topology-builder you need to implement a spout that subclasses BaseTrainingSpout  for feeding your training data storm.primitives.examples.TrainingSpout is an example) the rest of the template remains the same.

      MLTopologyBuilder builder = new MLTopologyBuilder();
      builder.setTrainingSpout(new TrainingSpout());

You'll then have a full fledged TrainingSpout that spits out a dataset of linearly separable data. Now we need to choose the learning algorithm. As this data is leanearly separable we can use the percpetron algorithm:

     builder.setTrainingBolt(new LocalPerceptronLearner());


We use the LocalLearner Bolt from  storm.primitives.examples.LocalPerceptronLearner.

     myTopology = builder.preopareTopology(1.0, 0.0, 0.5);

The first parameter passed to the builder is the bias, which is sometihing you can use to help you adjust your prediction. Then it is the threshold, this is normally 0.0, but you can customize it. Finally the learning rate, also usually 0.5 is the how fast the algorithm learns, but be warned learning too fast can result to destroy the current model and only take in consideration the latest training piece, so don't put it too high.

So let's recap we instiated the builder, then we plugged it up with a spout and a training bolt, we are ready to fire this up.

### Starting it

One thing to take into consideration is that we need to pass a new instance of a class that implements Idrpc to the MLTopologyBuilder for it to associate it with our topology. Once you do this you can use that drpc instance for making calls to the MLTopology.

        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("learning", conf, builder.createLocalTopology("evaluate", drpc));

        Utils.sleep(10000);
        cluster.killTopology("learning");
        cluster.shutdown();

### Predicting values using the drpc

You can execute code using the DRPC topology we previously built calling it like this: (please notice the nomenclature for a point to be classified):
    drpc.execute("evaluate", "[3.0, 2.0]");