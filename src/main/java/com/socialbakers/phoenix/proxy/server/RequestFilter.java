package com.socialbakers.phoenix.proxy.server;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;

public class RequestFilter extends ProtocolCodecFilter {

	private static final Logger LOGGER = LoggerFactory.getLogger(RequestFilter.class);

	static int maxRequestLen = 10 * 1024 * 1024; // 10 MB

	private static final int MSG_LEN_OF_INT = 4;

	public RequestFilter() {
		super(new RequestEncoder(), new RequestDecoder());
	}

	private static class RequestDecoder extends CumulativeProtocolDecoder {

		@Override
		protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {

			if (in.remaining() < MSG_LEN_OF_INT) {
				return false; // not even the request length can read
			}

			// reading the request length increments the position by four
			int initialPos = in.position();
			int requestLen = in.getInt();

			// heart beat request
			if (requestLen == Integer.MAX_VALUE) {
				LOGGER.trace("RECIEVED check request from {}", session.getRemoteAddress());
				session.write("ok");
				session.close(false);
				return true;
			}

			LOGGER.trace("Request length: {}", requestLen);

			if (requestLen <= 0) {
				throw new IllegalStateException("Message length must be greater than zero!");
			} else if (requestLen > maxRequestLen) {
				throw new IllegalStateException(String.format("Message length must be lower or equal than %d!",
						maxRequestLen));
			} else if (in.remaining() < requestLen) {
				// full request not available, move to initial position
				in.position(initialPos);
				return false;
			}

			in.position(initialPos + MSG_LEN_OF_INT);
			int endOfRequest = in.position() + requestLen;
			int maxPosition = in.limit();

			try {
				// bound and build request
				in.limit(endOfRequest);
				out.write(buildRequest(in.slice()));
			} finally {
				// always set position to next request
				in.position(endOfRequest);
				in.limit(maxPosition);
			}

			return true;
		}

		private PhoenixProxyProtos.QueryRequest buildRequest(IoBuffer in) throws InvalidProtocolBufferException {
			int limit = in.limit();
			byte[] bytes = new byte[limit];
			in.get(bytes);
			return PhoenixProxyProtos.QueryRequest.newBuilder().mergeFrom(bytes).build();
		}
	}

	private static class RequestEncoder extends ProtocolEncoderAdapter {

		@Override
		public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {

			IoBuffer ioBuffer;

			if (message instanceof PhoenixProxyProtos.QueryResponse) {
				byte[] bytes = ((PhoenixProxyProtos.QueryResponse) message).toByteArray();
				ioBuffer = IoBuffer.allocate(bytes.length + MSG_LEN_OF_INT, true);
				ioBuffer.putInt(bytes.length);
				ioBuffer.put(bytes);
			} else if (message instanceof String) {
				byte[] bytes = message.toString().getBytes();
				ioBuffer = IoBuffer.allocate(bytes.length, true);
				ioBuffer.put(bytes);
			} else {
				throw new IllegalArgumentException("Invalid typ of message: " + message.getClass().getName());
			}

			ioBuffer.flip(); // Flip it or there will be nothing to send
			out.write(ioBuffer);
			out.flush();
		}
	}

}
