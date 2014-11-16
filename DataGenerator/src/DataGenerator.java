import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class DataGenerator {
	static int count;

	public static void generateFile(int gb,int mb) {
		File file = new File("new.txt");
		/*try {
			FileWriter filewriter = new FileWriter(file);
			BufferedWriter writer = new BufferedWriter(filewriter);
			PrintWriter printer = new PrintWriter(writer);
			printer.append("hello");
			int i = 0;
			char c = 'a';
			for (int j = 0; j < size; j++ ) {

				printer.append(c);
				//System.out.println(c+" "+j);
				c++;
				if (c == 'z')
					c = 'a';
			}
			printer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		File src = new File("OSWI.txt");
		new File("data").mkdir();
		int counter=0;
		int gbcount=0, mbcount=0;
		gbcount = (int) ((gb*1024) / 2.6);
		mbcount = (int) (mb/2.6);
		File target ;
		for(int i=0;i< mbcount;i++ ) {
		

		try {
			target = new File("data/data"+counter+".txt");
			Files.copy(src.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
			counter++;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		}
		for(int i=0;i< gbcount;i++ ) {

			try {
				target = new File("data/data"+counter+".txt");
				Files.copy(src.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
				counter++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			}
	}

	public static void main(String[] args) {
		long start,end;
		
		//System.out.println((double)(end-start)/1000);
		if(args.length != 1) {
			System.out.println("Invalid argument length");
			return;
		}
		double size;
		int gb,mb;
		if(args.length == 1) {
			size = Double.parseDouble(args[0]);
			gb = (int)size;
			mb = (int) ((size - gb)*1024);
			generateFile(gb,mb);
		}

	}
}
