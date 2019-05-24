import java.util.*;
import java.io.*;

/* Wojciech Golab, University of Waterloo, 2019 */

public class Compare {
    
    public static void main(String[] args) throws Exception {
	HashMap<Integer, Integer> map0 = normalizeMap(fillMap(args[0]));
	HashMap<Integer, Integer> map1 = normalizeMap(fillMap(args[1]));

	if (map0.equals(map1)) {
	    System.out.println("Outputs equivalent.");
	    System.exit(0);
	} else {
	    System.out.println("Outputs NOT equivalent.");
	    System.exit(-1);
	}
    }

    static HashMap<Integer, List<Integer>> fillMap(String file) throws IOException {
	HashMap<Integer, List<Integer>> map = new HashMap<>();
	BufferedReader br = new BufferedReader(new FileReader(file));
	String line = null;
	while ((line = br.readLine()) != null) {
	    String[] verts = line.split("\\s+");
	    int a = Integer.parseInt(verts[0]);
	    int b = Integer.parseInt(verts[1]);
	    List<Integer> l = map.get(b);
	    if (l == null) {
		l = new ArrayList<>();
		map.put(b, l);
	    }
	    l.add(a);
	}
	br.close();
	return map;
    }

    static HashMap<Integer, Integer> normalizeMap(HashMap<Integer, List<Integer>> map) {
	HashMap<Integer, Integer> ret = new HashMap<>();
	for (Integer k: map.keySet()) {
	    int minVal = Collections.min(map.get(k));
	    for (Integer v: map.get(k)) {
		ret.put(v, minVal);
	    }
	}
	return ret;
    }
}
