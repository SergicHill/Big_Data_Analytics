package letters;


import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;



/**
 * written by Serg Khovansky
 * Using Flink technology, the code computes the number of letters in a given texts and compute regression beta in 
 * the regression Frequency of letters in text_y =a+beta* Frequency of letters in text_b
 * </ul>
 *
 */
public class LetterCount {

	

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input  data folders for two texts (i.e. two different folders) and output folder
		if(args.length !=6){
			System.err.println("Usage requires five input parameter: "
					+ "1) path to input folder x  path_infile_x,"
					+ "2) path to input folder y path_infile_y,"
					+ "3) path to output folder  path_outfiles,"
					+ "4) name of outfilename_x,"
					+ "5) name of outfilename_y,"
					+ "6) name of outfilename_beta_yx");
			System.exit(-1);
		}
		String pathInputText_x = args[0];
		String pathInputText_y = args[1];
		String pathOutputfolder =  args[2];
		String outputfilename_x = args[3];
		String outputfilename_y = args[4];
		String outputfile_beta_xy = args[5];
		
		// output files name
		String outputFile_x = "file://" + pathOutputfolder+"/"+ outputfilename_x;
		String outputFile_y = "file://" + pathOutputfolder+"/"+outputfilename_y; 
		String outputBeta = "file://" + pathOutputfolder+"/"+outputfile_beta_xy;
		
	
		//Work with two texts, hence, gets two DataSetsS
		DataSet<String> text_x = env.readTextFile(pathInputText_x);
		DataSet<String> text_y = env.readTextFile(pathInputText_y);

        
		//Get the splitter
        LineSplitter LS =  new LineSplitter(); 
		LS.set_mysplit("");
		
		//The list of letters for which to compute frequencies
		String abc = new String ("abcdefghijklmnopqrstuvwxyz");
		final String[] abc_array = abc.split("");
       
		// Start dataset transformations with text x
        DataSet<Tuple2<String, Integer>> maxcounts2_x = text_x
                 .flatMap(LS) // split the text into letters
				 .filter(    // filter out everything else beyond letters
				  new FilterFunction<Tuple2<String, Integer>>()
				    {
					  private static final long serialVersionUID = 1L;
					  @Override
				      public boolean filter(Tuple2<String, Integer> arg0) throws Exception
					  {
					    for (String s:  abc_array){ if(s.equals(arg0.f0)) return true;}
					    return false; 
				      }
					}
				   )
	               .groupBy(0)
	               .sum(1);
        
        // Start dataset transformations with text y
        DataSet<Tuple2<String, Integer>> maxcounts2_y = text_y
                .flatMap(LS) //  split the text into letters
				 .filter(   // filter out everything else beyond letters
				  new FilterFunction<Tuple2<String, Integer>>()
				    {
					  private static final long serialVersionUID = 1L;
					  @Override
				      public boolean filter(Tuple2<String, Integer> arg0) throws Exception
					  {
					    for (String s:  abc_array){ if(s.equals(arg0.f0)) return true;}
					    return false; 
				      }
					}
				   )
	               .groupBy(0) // group by the letter
	               .sum(1);  // sum up by the number of occurances

        //Find letter with max frequency
        DataSet<Tuple2<String, Integer>> maxcounts3_x = maxcounts2_x.aggregate(Aggregations.MAX, 1);
        DataSet<Tuple2<String, Integer>> maxcounts3_y = maxcounts2_y.aggregate(Aggregations.MAX, 1);
        

        
      // compute relative frequency for both texts
		DataSet<Tuple2<String, Double>>  relFreqCount_x = maxcounts2_x.cross(maxcounts3_x)
					.with(new ComputeRelFreq()).sortPartition(1,Order.DESCENDING ) ;
		 
		DataSet<Tuple2<String, Double>>  relFreqCount_y = maxcounts2_y.cross(maxcounts3_y)
				// compute relative frequency
				.with(new ComputeRelFreq()).sortPartition(1,Order.DESCENDING ) ;
		
		/** Strat computing regression beta:
		 * Freq_y=a+beta*Freq_x+error, where e.g. Freq_y is frequency of occurance of letters in text x
		 * beta=cov(Freq_y,Freq_x)/var(Freq_x)
		*/
		// compute sum of frequencies
		DataSet<Tuple2<String, Double>>  relFreqCount_x_sum = relFreqCount_x.aggregate(Aggregations.SUM, 1);
		DataSet<Tuple2<String, Double>>  relFreqCount_y_sum = relFreqCount_y.aggregate(Aggregations.SUM, 1);
		
		// Compute total number of used letters, should be 26 in large English texts
		DataSet<Tuple2<String, Double>> totalNumberLetters = relFreqCount_x.reduce(
				new ReduceFunction<Tuple2<String, Double>>()
		{
			private static final long serialVersionUID = 1L;
			
			@Override
			public Tuple2<String,  Double> reduce(Tuple2<String, Double> arg0, Tuple2<String, Double> arg1)
					throws Exception {
				Tuple2<String, Double> out = new Tuple2<String, Double>();
				//arg1.f1=1D;
				out.f0 ="count"; 
				out.f1 = arg0.f1+1D; 
				return out;
			}
			});

       //  compute average of relative Frequencies for text x		
		DataSet<Tuple2<String, Double>>  relFreqCount_x_average=relFreqCount_x_sum.cross(totalNumberLetters).with(
				new RatioFunc());
	//  compute average of relative Frequencies for text y
		DataSet<Tuple2<String, Double>>  relFreqCount_y_average=relFreqCount_y_sum.cross(totalNumberLetters).with(
				new RatioFunc());
	//  compute deviation from averages of relative Frequencies for text x	
		DataSet<Tuple2<String, Double>>  relFreqCount_x_dev = relFreqCount_x.cross(relFreqCount_x_average).with(
				new ComputeDeviation());
	//  compute deviation from averages of relative Frequencies for text y		
		DataSet<Tuple2<String, Double>>  relFreqCount_y_dev = relFreqCount_y.cross(relFreqCount_y_average).with(
				new ComputeDeviation());
		
		
		// make a DataSet with joint x and y frequencies (join on letters)	
		DataSet<Tuple2<Tuple2<String, Double>, Tuple2<String, Double>>> 	
		      relFreqCount_yx_dev = relFreqCount_y_dev.join(relFreqCount_x_dev)
	               .where(0)       // key of the first input (tuple field 0)
	               .equalTo(0);
		

	
        //Compute sum( x_dev^2)
		DataSet<Tuple2<String, Double>> XX_dev = relFreqCount_x_dev
				.map(new MapFunction<Tuple2<String, Double>, Tuple2<String, Double>>(){
						public Tuple2<String, Double>  map(Tuple2<String, Double> arg) { 
							Tuple2<String, Double> out = new Tuple2<String, Double>("",0D);
							out.f1=arg.f1*arg.f1;
							return out; } 
				}
						).aggregate(Aggregations.SUM, 1);
					
		//Compute sum( x_dev*y_dev)
   		  DataSet<Tuple2<String, Double>>   XY_dev  =
   				  relFreqCount_yx_dev.
   				map(new MapFunction< Tuple2< Tuple2<String, Double>, Tuple2<String, Double>>, Tuple2<String, Double> >(){

					@Override
					public Tuple2<String, Double> map(Tuple2<Tuple2<String, Double>, Tuple2<String, Double>> arg)
							throws Exception {
						Tuple2<String, Double> out = new Tuple2<String, Double>("",0D);				
						out.f1 = arg.f0.f1*arg.f1.f1;
						return out;
					} 
			}).aggregate(Aggregations.SUM, 1);
			

   		  // compute beta of the regression
  		DataSet<Tuple2<String, Double>>  beta = XX_dev.cross(XY_dev)
				.with(new ComputeBeta());
		
		
		 //beta.print();
		 beta.writeAsCsv(outputBeta);
		 
		 relFreqCount_x.writeAsCsv(outputFile_x);
		 relFreqCount_y.writeAsCsv(outputFile_y);
		 
		 
		 relFreqCount_x.print();
	
		

	}

	//
	// 	User Functions
	//


	 //Implements the Splitter	
	public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		DataSet<Tuple2<String, Integer>> A;
		Long myconst;
		String mysplit;
		LineSplitter(){mysplit="";}
		LineSplitter(String mysplit){this.mysplit= mysplit;}
		private static final long serialVersionUID = 1L;
		public void set_mysplit(String mysplit){
			this.mysplit=mysplit;
		}
		public void setTuple2(DataSet<Tuple2<String, Integer>> A){
			this.A=A;
			try {
				this.myconst= A.count();
			} catch (Exception e) {
				e.printStackTrace();
			}	
		}

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split(mysplit);

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					Tuple2<String, Integer> TP = new Tuple2<String, Integer>(token, 1);
					out.collect(TP);
				}
			}
		}

	}
	
	// compute ratio of one data over the other data, need to normalize frequencies
	public static class ComputeRelFreq implements CrossFunction <Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String,  Double>> {

		private static final long serialVersionUID = 1L;
		private double myscale;

		public ComputeRelFreq() {myscale=1L;}
		public void set_myscale(Long myscale){
			this.myscale = myscale;
		}

		public ComputeRelFreq(long myscale) {
			this.myscale = (double)myscale;
		}

		@Override
		public Tuple2<String, Double> cross(Tuple2<String, Integer> A, Tuple2<String, Integer> B) throws Exception {
			return new Tuple2<String, Double>(
					A.f0, 
					(double)(myscale*(double)A.f1/ (double)B.f1) // relative frequency
			);
		}
	}
	
	// Compute Beta
	public static class ComputeBeta implements CrossFunction <Tuple2<String, Double>, Tuple2<String, Double>,  Tuple2<String,  Double>> {

		private static final long serialVersionUID = 1L;
		public ComputeBeta() {}
		@Override
		public Tuple2<String, Double> cross(Tuple2<String, Double> arg0,
				Tuple2<String, Double> arg1) throws Exception {
			return new Tuple2<String, Double>(
					"beta",
					arg1.f1/arg0.f1
					) ;
		}
	};

    // Compute ratio of two functions
	public static class RatioFunc implements CrossFunction <Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String,  Double>> {

		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, Double> cross(Tuple2<String, Double> arg0,
				 Tuple2<String, Double> arg1) throws Exception {

			return new Tuple2<String, Double>(
					"Average",
					arg0.f1/arg1.f1
					) ;
		}
	};
	
	//ComputeDeviation()
	public static class ComputeDeviation implements CrossFunction <Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String,  Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Double> cross(Tuple2<String, Double> arg0,
				 Tuple2<String, Double> arg1) throws Exception {
				return new Tuple2<String, Double>(
					arg0.f0,
					arg0.f1 - arg1.f1
					) ;
		}
	};
	

}// last one
