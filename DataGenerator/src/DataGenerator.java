import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class DataGenerator {
	static int count;

	public void generateFile(int gb, int mb) {
		InputStream is = getClass().getResourceAsStream("OSWI.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();
		String temp;
		try {
			temp = reader.readLine();
			while (temp != null) {
				sb.append(temp);
				sb.append("\n");
				temp = reader.readLine();
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		int gbcount = 0, mbcount = 0;
		gbcount = (int) ((gb * 1024) / 2.6);
		mbcount = (int) (mb / 2.6);

		PrintWriter out;
		try {
			out = new PrintWriter(new FileWriter("DataFile.txt", true));
			for (int i = 0; i < mbcount; i++) {

				out.append(sb.toString());
				out.append("\n");

			}
			for (int i = 0; i < gbcount; i++) {

				out.append(sb.toString());
				out.append("\n");

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		// System.out.println((double)(end-start)/1000);
		if (args.length != 1) {
			System.out.println("Invalid argument length");
			return;
		}
		DataGenerator generator = new DataGenerator();
		double size;
		int gb, mb;
		if (args.length == 1) {
			size = Double.parseDouble(args[0]);
			gb = (int) size;
			mb = (int) ((size - gb) * 1024);
			generator.generateFile(gb, mb);
		}

	}
}
