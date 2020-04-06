package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.util.ArrayList;
import java.util.List;
import java.io.*;
import java.util.Scanner;

import static java.lang.Math.abs;
import static org.apache.spark.sql.functions.desc;


public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		//loading vertices
		java.util.List<Row> vertices_list = new ArrayList<Row>();
		try {
			File myVertices = new File("D:\\upc\\sdm\\lab2\\SparkGraphXassignment\\src\\main\\resources\\wiki-vertices.txt");
			Scanner myReader = new Scanner(myVertices);
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				String[] Vertices = data.split("\t");
				vertices_list.add(RowFactory.create(Vertices[0], Vertices[1]));
			}
			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		//loading vertices
		java.util.List<Row> edges_list = new ArrayList<Row>();
		try {
			File myEdges = new File("D:\\upc\\sdm\\lab2\\SparkGraphXassignment\\src\\main\\resources\\wiki-edges.txt");
			Scanner myReader = new Scanner(myEdges);
			while (myReader.hasNextLine()) {
				String data = myReader.nextLine();
				String[] Edges = data.split("\t");
				edges_list.add(RowFactory.create(Edges[0], Edges[1]));
			}
			myReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges);
		GraphFrame pr = gf.pageRank().maxIter(9).resetProbability(0.7).run();
//		pr.vertices().orderBy(desc("pagerank")).show();

		double eps = 0.001;
		double reset_prob=0;
		int max_iter=0;
		long start;
		long end;
		int flag=0; // used to check convergence

		List<Row> old_rank, new_rank;

		for (double j = 0.6; j < 1; j=j+0.05) {
			for (int i = 10; i < 15; ++i) {
				flag = 0;
				old_rank = pr.vertices().orderBy(desc("pagerank")).limit(10).collectAsList();
				start = System.nanoTime();
				pr = gf.pageRank().maxIter(i).resetProbability(j).run();
				end = System.nanoTime();
				new_rank = pr.vertices().orderBy(desc("pagerank")).limit(10).collectAsList();
//				pr.vertices().orderBy(desc("pagerank")).show();
				System.out.println("maxiter = " + i + " resetprob = "+j +": " + (long) (end - start) / 60_000_000_000.00);

				// checking if the top 10 stays the same and pageranks are the same
				for (int k=0; k<10; ++k) {
					if (old_rank.get(k).get(0).toString().equals(new_rank.get(k).get(0).toString()) &&
							abs(((double) old_rank.get(k).get(2)) - (double) new_rank.get(k).get(2)) <= eps) {
						flag++;
					}
					else
						break;
				}

				if (flag==10 ) {
					max_iter = i;
					break;
				}
				}

			if(flag==10) {
				reset_prob = j;
				break;
			}
			}
		System.out.println("max iteration = " + max_iter + ", reset probability = " + reset_prob);
		System.out.println("10 most relevant articles");
		pr.vertices().orderBy(desc("pagerank")).limit(10).show();
		}

	}

