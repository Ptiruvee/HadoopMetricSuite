import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class DataGenerator {
	static int count;

	public static void generateFile(int size) {
		File file = new File("new.txt");
		try {
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
		}

	}

	public static void main(String[] args) {
		long start,end;
		start=System.currentTimeMillis();
		generateFile(1024*1024*10);
		end=System.currentTimeMillis();
		System.out.println((double)(end-start)/1000);
		File src = new File("OSWI.txt");
		//new File("data").mkdir();
		//int replication;
		File target = new File("data/copy.txt");

		/*try {
			Files.copy(src.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

	}
}
