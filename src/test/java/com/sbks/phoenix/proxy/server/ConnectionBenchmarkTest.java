package com.sbks.phoenix.proxy.server;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.Socket;

public class ConnectionBenchmarkTest {

	private static final int REQ_COUNT = 4; // number of requests in one connection
	private static final int CONN_COUNT = 64; // number of connections in one client thread
	private static final int THREAD_COUNT = 16; // number of parallel client threads

	private static final byte[] COMPLETE_MSG = new byte[] { 0x00, 0x00, 0x00, 0x02, 0x08, 0x01 };
	private static final byte[] INCOMPLETE_MSG = new byte[] { 0x00, 0x00, 0x00, 0x03, 0x08, 0x01 };

	private static int ERR_COUNT = 0;

	public static void main(String[] args) throws InterruptedException {

		try {
			Socket socket = new Socket("localhost", 8989);
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.write(INCOMPLETE_MSG);
			// close before send complete message
			socket.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

		Thread mainThread = new Thread() {
			@Override
			public void run() {
				for (int t = 0; t < THREAD_COUNT; t++) {

					final int t1 = t;

					Thread inThread = new Thread() {
						@Override
						public void run() {
							for (int c = 0; c < CONN_COUNT; c++) {
								Socket socket;
								try {

									socket = new Socket("localhost", 8989);
									DataOutputStream dos = new DataOutputStream(socket.getOutputStream());

									for (int r = 0; r < REQ_COUNT; r++) {
										// write request
										dos.write(COMPLETE_MSG);
										Thread.sleep(new Double(Math.random() * 1000).longValue());
									}

									for (int r = 0; r < REQ_COUNT; r++) {
										// read response
										InputStream inputStream = socket.getInputStream();
										int read = 0;
										byte[] msg = new byte[COMPLETE_MSG.length];
										while (read < COMPLETE_MSG.length) {
											read += inputStream.read(msg, read, msg.length - read);
										}
										Thread.sleep(new Double(Math.random() * 1000).longValue());
									}

									socket.close();
									System.out.println("T" + t1 + " C" + c + " done.");

								} catch (Exception e) {
									ERR_COUNT++;
									System.err.println("Problem with " + c + ". conn in thread " + t1 + ".\n"
											+ e.getMessage());
								}
							}
							System.out.println("T" + t1 + " done.");
							System.out.println("Total Err count " + ERR_COUNT);
						}
					};

					inThread.start();
				}
			}
		};
		mainThread.start();
		mainThread.join();
	}

}
