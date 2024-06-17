package org.example;

import java.io.IOException;

public class Client {
	public static void main(String[] args) throws IOException {
		String address = "localhost:2181"; // Replace with the address to your ZooKeeper server

		Library library = new Library(address, 3000);

		// Key must begin with a / character
		library.put("/node2", "node 2 data");
		System.out.println(library.get("/node2"));
	}
}
