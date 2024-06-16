package org.example;

import java.io.IOException;

public class Client {
	public static void main(String[] args) throws IOException {
		Library library = new Library("localhost:2181", 3000);

		library.put("/node2", "node 2 data");
		System.out.println(library.get("/node2"));
	}
}
