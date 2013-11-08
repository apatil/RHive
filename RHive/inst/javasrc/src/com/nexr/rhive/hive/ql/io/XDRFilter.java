package com.nexr.rhive.hive.ql.io;

import hep.io.xdr.XDROutputStream;

import java.io.ByteArrayOutputStream;

public class XDRFilter extends XDROutputStream {
	private ByteArrayOutputStream buffer;
	
	public XDRFilter(ByteArrayOutputStream buffer) {
		super(buffer);
		this.buffer = buffer;
	}

	public byte[] getData() {
		return buffer.toByteArray();
	}
}
