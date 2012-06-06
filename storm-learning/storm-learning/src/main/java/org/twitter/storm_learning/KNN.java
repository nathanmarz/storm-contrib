package org.twitter.storm_learning;

import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Math;
import static fj.data.List.list;  

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector;

import fj.F;  
import fj.data.List;  
import fj.data.Array;  
import static fj.function.Doubles.add;


public class KNN {

	private DenseMatrix trainingData;

	public KNN (DenseMatrix data) { 
		this.trainingData = data;
	}

	public List<Point> matrixToArray(DenseMatrix matrix){
		Iterator<MatrixSlice> trainingIterator = matrix.iterator();
		while(trainingIterator.hasNext())
		{
			//trainingData2 = trainingIterator.next().vector();
		}
		//TODO fix mocked out for the time being
		return list(new Point(), new Point());
	}

	public List<Double> eucludianDistance(Integer nbrNeighbors, final Point myPoint){
		List<Point> a = matrixToArray(trainingData);
		return a.map(new F<Point, Double>(){
			// get euclidian distance
			@Override
			public Double f(Point neighbor) {
				return java.lang.Math.sqrt(java.lang.Math.pow(neighbor.getX()  - myPoint.getX(), 2.0)
						+ java.lang.Math.pow(neighbor.getY() - myPoint.getY(),2.0));
			}
		}).take(nbrNeighbors);


	}

	public Double executeLabeling(Integer nbrNeighbors, Point point) {
		List<Double> distances = eucludianDistance(nbrNeighbors, point);
		final Double sumDistance = distances.foldLeft(add,0.0);
		return sumDistance/(Double)sumDistance;
	}

}
