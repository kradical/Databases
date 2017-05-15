import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Comparator;

public class SortPost {
	String filename_in;
	String filename_out;
	int M; //size of chunk to sort in number of lines, also the size of a sorted sublist
	int B; //size of buffer in characters (1 char = 2 bytes), if B==8192 char, it is 16K bytes
	int c; //sort column
	String sep; //column separator
	String tmpfileprefix = "tmpfile";
	
	int numChunks = 0;
	
	public SortPost(String filename_in, String filename_out, int M, int B, int c, String sep) {
		this.filename_in  = filename_in;
		this.filename_out = filename_out;
		
		this.M = M;
		this.B = B;
		
		this.c = c;
		this.sep = sep;
	}
	
	public void doSort() throws Exception {
		//Phase 1
		long startTime = System.currentTimeMillis();
		System.out.println("Phase 1 started");
		BufferedReader in = new BufferedReader( new FileReader(filename_in) );
        
        int cnt = 0;
        String[] chunk = new String[M];

		for(;;) {
			String line;
			while((line = in.readLine()) != null && cnt < M) {
				chunk[cnt++] = line;
			}

			Arrays.sort(chunk, new Comparator<String>() {
				public int compare(String st1, String st2) {
					String cmp1 = st1.split("\\t")[c]; // get the correct comparison column
					String cmp2 = st2.split("\\t")[c]; // get the correct comparison column

					return cmp1.compareTo(cmp2);
				}
			});

			// write to temp file
			BufferedWriter out = new BufferedWriter( new FileWriter(tmpfileprefix + numChunks), B );
			for(int i=0; i<chunk.length; i++) {
				out.write(chunk[i] + '\n');
				// System.out.println(chunk[i] + '\n');
			}
			out.close();
			cnt = 0;
			numChunks++;

			if(line == null) { break; }
		}
		
        
        in.close();
		System.out.println("Phase 1 Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
        
        
        //Phase 2
		startTime = System.currentTimeMillis();
        System.out.println("Phase 2 started");
        
        //We have numChunks sorted sublists that we need to merge.
        //We don't need to do buffer management directly.
        //Buffer management is done by BufferedReader.
		BufferedReader[] readers = new BufferedReader[numChunks];
		
		//We will use a Priority Queue to implement efficiently 
		//the competition among the heads of the sorted sublists (chunks).
		//We create a head-index pair to be used as the type for 
		//the elements of the Priority Queue. 
		//The index is the number of the sorted sublist the head belongs to. 
		//Why do we need the index? 
		//When a head "wins" (being the smallest among heads), 
		//it "migrates" to the output buffer. As such, we need to 
		//insert a new head from the corresponding sublist (chunk), 
		//and the index tells us which sublist that is.
		class HeadIndexPair { 
			String head; 
			int i; 
			HeadIndexPair(String head, int i) {
				this.head = head;
				this.i = i;
			}
		}
		
		PriorityQueue<HeadIndexPair> heads = 
				new PriorityQueue<>(numChunks, (a,b) -> compare(a.head,b.head));
		
		for(int i=0; i<numChunks; i++)
			readers[i] = new BufferedReader( new FileReader(tmpfileprefix+i), B );
		
		BufferedWriter out = new BufferedWriter( new FileWriter(filename_out), B );
		
		for(int i=0; i<numChunks; i++)
			heads.add( new HeadIndexPair(readers[i].readLine(), i) ); 
		
		while(true) {
			HeadIndexPair minh = heads.poll();
			//TODO: Complete the merge phase
			//If what you get from poll is null, it means the sublists are exhausted, 
			//so time to break from this while loop.
			if (minh == null) {
				break;
			}
			//Otherwise, add head to output, and insert the new head from the 
			//sublist into the Priority queue.
			out.write(minh.head + '\n');
			String nextVal = readers[minh.i].readLine();
			// System.out.println(nextVal);
			if(nextVal != null){
				heads.add(new HeadIndexPair(nextVal, minh.i));
			}
		}
		for(int i=0; i<numChunks; i++)
			readers[i].close();
		out.close();
		System.out.println("Phase 2 Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
		System.out.println("Sort complete");
	}
	
	void sortAndSaveChunk(String[] chunk, String filename) throws Exception {
		System.out.println("sorting and saving " + filename);
		
		Arrays.parallelSort(chunk, (a,b) -> compare(a,b) );
		
		BufferedWriter out = new BufferedWriter( new FileWriter(filename) );
		
		for(int i=0; i<chunk.length; i++) {
			if(chunk[i] != null) {
				out.write(chunk[i]);
				out.newLine();
			}
		}
		
		out.close();
	}
	
	String extractCol(String line) {
		String[] columns = line.split(sep);
		return columns[c];
	}
	
	int compare(String a, String b) {
		if(a==null && b==null)
			return 0;
		if (a==null)
			return 1;
		if (b==null)
			return -1;
		return extractCol(a).compareTo(extractCol(b));
	}
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		//taxpayers_30.txt is a file containing only 30 records. 
		//This is to make sure your program works correctly. 
		SortPost mysort = new SortPost("taxpayers_30.txt", "taxpayers_30_sorted.txt", 10, 8192, 2, "\t");
		
		/*
		//Only do this when you are sure the program works fine.
		//The sort of such a file could take several minutes 
		//depending on your machine (be patient). 
		SortPost mysort = new SortPost(
				"taxpayers_3M.txt", 		//input file 
				"taxpayers_3M_sorted.txt",	//output sorted file
				300_000,					//M, size of chunk to sort in number of lines, also the size of a sorted sublist
				8192, 						//B, size of buffer in characters (1 char = 2 bytes), if B==8192 char, it is 16K bytes
				0,							//c, sort column 
				"\t"						//sep, column separator
				);
		*/
		
		try {
			mysort.doSort();
		} catch(Exception e) {
			System.out.println(e);
		}
		
		System.out.println("Time elapsed (sec) = " + (System.currentTimeMillis() - startTime) / 1000.0);
	}
}